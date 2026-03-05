import { InstanceDto } from '@api/dto/instance.dto';
import { ChatwootDto } from '@api/integrations/chatbot/chatwoot/dto/chatwoot.dto';
import { postgresClient } from '@api/integrations/chatbot/chatwoot/libs/postgres.client';
import { ChatwootService } from '@api/integrations/chatbot/chatwoot/services/chatwoot.service';
import { Logger } from '@config/logger.config';
import { inbox } from '@figuro/chatwoot-sdk';
import { Chatwoot as ChatwootModel, Contact, Message } from '@prisma/client';
import { proto } from 'baileys';

type ChatwootUser = {
  user_type: string;
  user_id: number;
};

type FksChatwoot = {
  contact_key: string;
  phone_number: string;
  contact_id: string;
  conversation_id: string;
};

type firstLastTimestamp = {
  first: number;
  last: number;
};

type IWebMessageInfo = Omit<proto.IWebMessageInfo, 'key'> & Partial<Pick<proto.IWebMessageInfo, 'key'>>;

class ChatwootImport {
  private logger = new Logger('ChatwootImport');
  private repositoryMessagesCache = new Map<string, Set<string>>();
  private historyMessages = new Map<string, Message[]>();
  private historyContacts = new Map<string, Contact[]>();

  public getRepositoryMessagesCache(instance: InstanceDto) {
    return this.repositoryMessagesCache.has(instance.instanceName)
      ? this.repositoryMessagesCache.get(instance.instanceName)
      : null;
  }

  public setRepositoryMessagesCache(instance: InstanceDto, repositoryMessagesCache: Set<string>) {
    this.repositoryMessagesCache.set(instance.instanceName, repositoryMessagesCache);
  }

  public deleteRepositoryMessagesCache(instance: InstanceDto) {
    this.repositoryMessagesCache.delete(instance.instanceName);
  }

  public addHistoryMessages(instance: InstanceDto, messagesRaw: Message[]) {
    const actualValue = this.historyMessages.has(instance.instanceName)
      ? this.historyMessages.get(instance.instanceName)
      : [];
    this.historyMessages.set(instance.instanceName, [...actualValue, ...messagesRaw]);
  }

  public addHistoryContacts(instance: InstanceDto, contactsRaw: Contact[]) {
    const actualValue = this.historyContacts.has(instance.instanceName)
      ? this.historyContacts.get(instance.instanceName)
      : [];
    this.historyContacts.set(instance.instanceName, actualValue.concat(contactsRaw));
  }

  public deleteHistoryMessages(instance: InstanceDto) {
    this.historyMessages.delete(instance.instanceName);
  }

  public deleteHistoryContacts(instance: InstanceDto) {
    this.historyContacts.delete(instance.instanceName);
  }

  public clearAll(instance: InstanceDto) {
    this.deleteRepositoryMessagesCache(instance);
    this.deleteHistoryMessages(instance);
    this.deleteHistoryContacts(instance);
  }

  public getHistoryMessagesLenght(instance: InstanceDto) {
    return this.historyMessages.get(instance.instanceName)?.length ?? 0;
  }

  public async importHistoryContacts(instance: InstanceDto, provider: ChatwootDto) {
    try {
      if (this.getHistoryMessagesLenght(instance) > 0) {
        return;
      }

      const pgClient = postgresClient.getChatwootConnection();

      let totalContactsImported = 0;

      const contacts = this.historyContacts.get(instance.instanceName) || [];
      if (contacts.length === 0) {
        return 0;
      }

      let contactsChunk: Contact[] = this.sliceIntoChunks(contacts, 3000);
      while (contactsChunk.length > 0) {
        const labelSql = `SELECT id FROM labels WHERE title = '${provider.nameInbox}' AND account_id = ${provider.accountId} LIMIT 1`;

        let labelId = (await pgClient.query(labelSql))?.rows[0]?.id;

        if (!labelId) {
          // creating label in chatwoot db and getting the id
          const sqlLabel = `INSERT INTO labels (title, color, show_on_sidebar, account_id, created_at, updated_at) VALUES ('${provider.nameInbox}', '#34039B', true, ${provider.accountId}, NOW(), NOW()) RETURNING id`;

          labelId = (await pgClient.query(sqlLabel))?.rows[0]?.id;
        }

        // inserting contacts in chatwoot db
        let sqlInsert = `INSERT INTO contacts
          (name, phone_number, account_id, identifier, created_at, updated_at) VALUES `;
        const bindInsert = [provider.accountId];

        for (const contact of contactsChunk) {
          const isGroup = this.isIgnorePhoneNumber(contact.remoteJid);

          const contactName = isGroup ? `${contact.pushName} (GROUP)` : contact.pushName;
          bindInsert.push(contactName);
          const bindName = `$${bindInsert.length}`;

          let bindPhoneNumber: string;
          if (!isGroup) {
            bindInsert.push(`+${contact.remoteJid.split('@')[0]}`);
            bindPhoneNumber = `$${bindInsert.length}`;
          } else {
            bindPhoneNumber = 'NULL';
          }
          bindInsert.push(contact.remoteJid);
          const bindIdentifier = `$${bindInsert.length}`;

          sqlInsert += `(${bindName}, ${bindPhoneNumber}, $1, ${bindIdentifier}, NOW(), NOW()),`;
        }
        if (sqlInsert.slice(-1) === ',') {
          sqlInsert = sqlInsert.slice(0, -1);
        }
        sqlInsert += ` ON CONFLICT (identifier, account_id)
                       DO UPDATE SET
                        name = EXCLUDED.name,
                        phone_number = EXCLUDED.phone_number,
                        updated_at = NOW()`;

        totalContactsImported += (await pgClient.query(sqlInsert, bindInsert))?.rowCount ?? 0;

        const sqlTags = `SELECT id FROM tags WHERE name = '${provider.nameInbox}' LIMIT 1`;

        const tagData = (await pgClient.query(sqlTags))?.rows[0];
        let tagId = tagData?.id;

        const sqlTag = `INSERT INTO tags (name, taggings_count) VALUES ('${provider.nameInbox}', ${totalContactsImported}) ON CONFLICT (name) DO UPDATE SET taggings_count = tags.taggings_count + ${totalContactsImported} RETURNING id`;

        tagId = (await pgClient.query(sqlTag))?.rows[0]?.id;

        await pgClient.query(sqlTag);

        let sqlInsertLabel = `INSERT INTO taggings (tag_id, taggable_type, taggable_id, context, created_at) VALUES `;

        contactsChunk.forEach((contact) => {
          const bindTaggableId = `(SELECT id FROM contacts WHERE identifier = '${contact.remoteJid}' AND account_id = ${provider.accountId})`;
          sqlInsertLabel += `($1, $2, ${bindTaggableId}, $3, NOW()),`;
        });

        if (sqlInsertLabel.slice(-1) === ',') {
          sqlInsertLabel = sqlInsertLabel.slice(0, -1);
        }

        await pgClient.query(sqlInsertLabel, [tagId, 'Contact', 'labels']);

        contactsChunk = this.sliceIntoChunks(contacts, 3000);
      }

      this.deleteHistoryContacts(instance);

      return totalContactsImported;
    } catch (error) {
      this.logger.error(`Error on import history contacts: ${error.toString()}`);
    }
  }

  public async getExistingSourceIds(sourceIds: string[], conversationId?: number): Promise<Set<string>> {
    try {
      const existingSourceIdsSet = new Set<string>();

      if (sourceIds.length === 0) {
        return existingSourceIdsSet;
      }

      // Ensure all sourceIds are consistently prefixed with 'WAID:' as required by downstream systems and database queries.
      const formattedSourceIds = sourceIds.map((sourceId) => `WAID:${sourceId.replace('WAID:', '')}`);
      const pgClient = postgresClient.getChatwootConnection();

      const params = conversationId ? [formattedSourceIds, conversationId] : [formattedSourceIds];

      const query = conversationId
        ? 'SELECT source_id FROM messages WHERE source_id = ANY($1) AND conversation_id = $2'
        : 'SELECT source_id FROM messages WHERE source_id = ANY($1)';

      const result = await pgClient.query(query, params);
      for (const row of result.rows) {
        existingSourceIdsSet.add(row.source_id);
      }

      return existingSourceIdsSet;
    } catch (error) {
      this.logger.error(`Error on getExistingSourceIds: ${error.toString()}`);
      return new Set<string>();
    }
  }

  public async importHistoryMessages(
    instance: InstanceDto,
    chatwootService: ChatwootService,
    inbox: inbox,
    provider: ChatwootModel,
  ) {
    try {
      const pgClient = postgresClient.getChatwootConnection();

      const chatwootUser = await this.getChatwootUser(provider);
      if (!chatwootUser) {
        throw new Error('User not found to import messages.');
      }

      let totalMessagesImported = 0;

      let messagesOrdered = this.historyMessages.get(instance.instanceName) || [];
      if (messagesOrdered.length === 0) {
        return 0;
      }

      // ordering messages by number and timestamp asc
      messagesOrdered.sort((a, b) => {
        const aKey = a.key as {
          remoteJid: string;
        };

        const bKey = b.key as {
          remoteJid: string;
        };

        const aMessageTimestamp = a.messageTimestamp as any as number;
        const bMessageTimestamp = b.messageTimestamp as any as number;

        return parseInt(aKey.remoteJid) - parseInt(bKey.remoteJid) || aMessageTimestamp - bMessageTimestamp;
      });

      const allMessagesMappedByRemoteJid = this.createMessagesMapByPhoneNumber(messagesOrdered);
      // Map structure: remoteJid => { first, last } timestamps (supports @lid and @s.whatsapp.net separately)
      const remoteJidsWithTimestamp = new Map<string, firstLastTimestamp>();
      allMessagesMappedByRemoteJid.forEach((messages: Message[], remoteJid: string) => {
        remoteJidsWithTimestamp.set(remoteJid, {
          first: messages[0]?.messageTimestamp as any as number,
          last: messages[messages.length - 1]?.messageTimestamp as any as number,
        });
      });

      const existingSourceIds = await this.getExistingSourceIds(messagesOrdered.map((message: any) => message.key.id));
      messagesOrdered = messagesOrdered.filter((message: any) => !existingSourceIds.has(`WAID:${message.key.id}`));
      // processing messages in batch
      const batchSize = 4000;
      let messagesChunk: Message[] = this.sliceIntoChunks(messagesOrdered, batchSize);
      while (messagesChunk.length > 0) {
        // Map structure: remoteJid => Message[] (supports @lid and @s.whatsapp.net separately)
        const messagesByRemoteJid = this.createMessagesMapByPhoneNumber(messagesChunk);

        if (messagesByRemoteJid.size > 0) {
          const fksByRemoteJid = await this.selectOrCreateFksFromChatwoot(
            provider,
            inbox,
            remoteJidsWithTimestamp,
            messagesByRemoteJid,
          );

          // inserting messages in chatwoot db
          let sqlInsertMsg = `INSERT INTO messages
            (content, processed_message_content, account_id, inbox_id, conversation_id, message_type, private, content_type,
            sender_type, sender_id, source_id, created_at, updated_at) VALUES `;
          const bindInsertMsg = [provider.accountId, inbox.id];

          messagesByRemoteJid.forEach((messages: any[], remoteJid: string) => {
            const fksChatwoot = fksByRemoteJid.get(remoteJid);

            messages.forEach((message) => {
              if (!message.message) {
                return;
              }

              if (!fksChatwoot?.conversation_id || !fksChatwoot?.contact_id) {
                return;
              }

              const contentMessage = this.getContentMessage(chatwootService, message);
              if (!contentMessage) {
                return;
              }

              bindInsertMsg.push(contentMessage);
              const bindContent = `$${bindInsertMsg.length}`;

              bindInsertMsg.push(fksChatwoot.conversation_id);
              const bindConversationId = `$${bindInsertMsg.length}`;

              bindInsertMsg.push(message.key.fromMe ? '1' : '0');
              const bindMessageType = `$${bindInsertMsg.length}`;

              bindInsertMsg.push(message.key.fromMe ? chatwootUser.user_type : 'Contact');
              const bindSenderType = `$${bindInsertMsg.length}`;

              bindInsertMsg.push(message.key.fromMe ? chatwootUser.user_id : fksChatwoot.contact_id);
              const bindSenderId = `$${bindInsertMsg.length}`;

              bindInsertMsg.push('WAID:' + message.key.id);
              const bindSourceId = `$${bindInsertMsg.length}`;

              bindInsertMsg.push(message.messageTimestamp as number);
              const bindmessageTimestamp = `$${bindInsertMsg.length}`;

              sqlInsertMsg += `(${bindContent}, ${bindContent}, $1, $2, ${bindConversationId}, ${bindMessageType}, FALSE, 0,
                  ${bindSenderType},${bindSenderId},${bindSourceId}, to_timestamp(${bindmessageTimestamp}), to_timestamp(${bindmessageTimestamp})),`;
            });
          });
          if (bindInsertMsg.length > 2) {
            if (sqlInsertMsg.slice(-1) === ',') {
              sqlInsertMsg = sqlInsertMsg.slice(0, -1);
            }
            totalMessagesImported += (await pgClient.query(sqlInsertMsg, bindInsertMsg))?.rowCount ?? 0;
          }
        }
        messagesChunk = this.sliceIntoChunks(messagesOrdered, batchSize);
      }

      this.deleteHistoryMessages(instance);
      this.deleteRepositoryMessagesCache(instance);

      const providerData: ChatwootDto = {
        ...provider,
        ignoreJids: Array.isArray(provider.ignoreJids) ? provider.ignoreJids.map((event) => String(event)) : [],
      };

      this.importHistoryContacts(instance, providerData);

      return totalMessagesImported;
    } catch (error) {
      this.logger.error(`Error on import history messages: ${error.toString()}`);

      this.deleteHistoryMessages(instance);
      this.deleteRepositoryMessagesCache(instance);
    }
  }

  /**
   * Select or create contacts/conversations in Chatwoot.
   * Uses full remoteJid as identifier to support @lid vs @s.whatsapp.net as separate conversations.
   */
  public async selectOrCreateFksFromChatwoot(
    provider: ChatwootModel,
    inbox: inbox,
    remoteJidsWithTimestamp: Map<string, firstLastTimestamp>,
    messagesByRemoteJid: Map<string, Message[]>,
  ): Promise<Map<string, FksChatwoot>> {
    const pgClient = postgresClient.getChatwootConnection();

    const bindValues: any[] = [provider.accountId, inbox.id];
    const valueRows: string[] = [];

    for (const remoteJid of messagesByRemoteJid.keys()) {
      const ts = remoteJidsWithTimestamp.get(remoteJid);
      if (!ts) continue;

      const phonePart = remoteJid.split('@')[0];
      const phoneNumber = `+${phonePart}`;

      const base = bindValues.length + 1;
      bindValues.push(remoteJid, phoneNumber, ts.first, ts.last);
      valueRows.push(`($${base}, $${base + 1}, $${base + 2}, $${base + 3})`);
    }

    if (valueRows.length === 0) {
      return new Map();
    }

    const sqlFromChatwoot = `WITH
              contact_input AS (
                SELECT identifier, phone_number, created_at::INTEGER, last_activity_at::INTEGER FROM (
                  VALUES ${valueRows.join(', ')}
                ) as t (identifier, phone_number, created_at, last_activity_at)
              ),

              only_new AS (
                SELECT * FROM contact_input
                WHERE identifier NOT IN (
                  SELECT c.identifier FROM contacts c
                    JOIN contact_inboxes ci ON ci.contact_id = c.id AND ci.inbox_id = $2
                    JOIN conversations con ON con.contact_inbox_id = ci.id
                      AND con.account_id = $1 AND con.inbox_id = $2 AND con.contact_id = c.id
                  WHERE c.account_id = $1
                )
              ),

              new_contact AS (
                INSERT INTO contacts (name, phone_number, account_id, identifier, created_at, updated_at)
                SELECT REPLACE(p.phone_number, '+', ''), p.phone_number, $1, p.identifier,
                  to_timestamp(p.created_at), to_timestamp(p.last_activity_at)
                FROM only_new AS p
                ON CONFLICT(identifier, account_id) DO UPDATE SET updated_at = EXCLUDED.updated_at
                RETURNING id, identifier, phone_number, created_at, updated_at
              ),

              new_contact_inbox AS (
                INSERT INTO contact_inboxes (contact_id, inbox_id, source_id, created_at, updated_at)
                SELECT new_contact.id, $2, gen_random_uuid(), new_contact.created_at, new_contact.updated_at
                FROM new_contact
                RETURNING id, contact_id, created_at, updated_at
              ),

              new_conversation AS (
                INSERT INTO conversations (account_id, inbox_id, status, contact_id,
                  contact_inbox_id, uuid, last_activity_at, created_at, updated_at)
                SELECT $1, $2, 0, new_contact_inbox.contact_id, new_contact_inbox.id, gen_random_uuid(),
                  new_contact_inbox.updated_at, new_contact_inbox.created_at, new_contact_inbox.updated_at
                FROM new_contact_inbox
                RETURNING id, contact_id
              )

              SELECT new_contact.identifier AS contact_key, new_contact.phone_number,
                new_contact.id AS contact_id, new_conversation.id AS conversation_id
              FROM new_conversation
              JOIN new_contact ON new_conversation.contact_id = new_contact.id

              UNION

              SELECT p.identifier AS contact_key, p.phone_number, c.id AS contact_id, con.id AS conversation_id
              FROM contact_input p
              JOIN contacts c ON c.identifier = p.identifier AND c.account_id = $1
              JOIN contact_inboxes ci ON ci.contact_id = c.id AND ci.inbox_id = $2
              JOIN conversations con ON con.contact_inbox_id = ci.id AND con.account_id = $1
                AND con.inbox_id = $2 AND con.contact_id = c.id`;

    const fksFromChatwoot = await pgClient.query(sqlFromChatwoot, bindValues);

    return new Map(fksFromChatwoot.rows.map((item: FksChatwoot) => [item.contact_key, item]));
  }

  public async getChatwootUser(provider: ChatwootModel): Promise<ChatwootUser> {
    try {
      const pgClient = postgresClient.getChatwootConnection();

      const sqlUser = `SELECT owner_type AS user_type, owner_id AS user_id
                         FROM access_tokens
                       WHERE token = $1`;

      return (await pgClient.query(sqlUser, [provider.token]))?.rows[0] || false;
    } catch (error) {
      this.logger.error(`Error on getChatwootUser: ${error.toString()}`);
    }
  }

  /**
   * Groups messages by full remoteJid to support @lid vs @s.whatsapp.net as separate conversations.
   * Key = full remoteJid (e.g. "26998801960985@lid" or "553899316490@s.whatsapp.net")
   */
  public createMessagesMapByPhoneNumber(messages: Message[]): Map<string, Message[]> {
    return messages.reduce((acc: Map<string, Message[]>, message: Message) => {
      const key = message?.key as { remoteJid: string };
      const remoteJid = key?.remoteJid;
      if (remoteJid && !this.isIgnorePhoneNumber(remoteJid)) {
        const existing = acc.has(remoteJid) ? acc.get(remoteJid) : [];
        existing.push(message);
        acc.set(remoteJid, existing);
      }
      return acc;
    }, new Map());
  }

  public async getContactsOrderByRecentConversations(
    inbox: inbox,
    provider: ChatwootModel,
    limit = 50,
  ): Promise<{ id: number; phone_number: string; identifier: string }[]> {
    try {
      const pgClient = postgresClient.getChatwootConnection();

      const sql = `SELECT contacts.id, contacts.identifier, contacts.phone_number
                     FROM conversations
                   JOIN contacts ON contacts.id = conversations.contact_id
                   WHERE conversations.account_id = $1
                     AND inbox_id = $2
                   ORDER BY conversations.last_activity_at DESC
                   LIMIT $3`;

      return (await pgClient.query(sql, [provider.accountId, inbox.id, limit]))?.rows;
    } catch (error) {
      this.logger.error(`Error on get recent conversations: ${error.toString()}`);
    }
  }

  public getContentMessage(chatwootService: ChatwootService, msg: IWebMessageInfo) {
    const contentMessage = chatwootService.getConversationMessage(msg.message);
    if (contentMessage) {
      return contentMessage;
    }

    // Always use placeholder for media/unknown types so we never skip messages during import
    const types = {
      documentMessage: msg.message.documentMessage,
      documentWithCaptionMessage: msg.message.documentWithCaptionMessage?.message?.documentMessage,
      imageMessage: msg.message.imageMessage,
      videoMessage: msg.message.videoMessage,
      audioMessage: msg.message.audioMessage,
      stickerMessage: msg.message.stickerMessage,
      templateMessage: msg.message.templateMessage?.hydratedTemplate?.hydratedContentText,
      reactionMessage: msg.message.reactionMessage,
      protocolMessage: msg.message.protocolMessage,
      viewOnceMessageV2: msg.message.viewOnceMessageV2,
      ephemeralMessage: msg.message.ephemeralMessage,
    };

    const typeKey = Object.keys(types).find((key) => types[key] !== undefined && types[key] !== null);
    switch (typeKey) {
      case 'documentMessage': {
        const doc = msg.message.documentMessage;
        const fileName = doc?.fileName || 'document';
        const caption = doc?.caption ? ` ${doc.caption}` : '';
        return `_<File: ${fileName}${caption}>_`;
      }

      case 'documentWithCaptionMessage': {
        const doc = msg.message.documentWithCaptionMessage?.message?.documentMessage;
        const fileName = doc?.fileName || 'document';
        const caption = doc?.caption ? ` ${doc.caption}` : '';
        return `_<File: ${fileName}${caption}>_`;
      }

      case 'templateMessage': {
        const template = msg.message.templateMessage?.hydratedTemplate;
        return (
          (template?.hydratedTitleText ? `*${template.hydratedTitleText}*\n` : '') +
          (template?.hydratedContentText || '_<Template Message>_')
        );
      }

      case 'imageMessage':
        return '_<Image Message>_';

      case 'videoMessage':
        return '_<Video Message>_';

      case 'audioMessage':
        return '_<Audio Message>_';

      case 'stickerMessage':
        return '_<Sticker Message>_';

      case 'reactionMessage':
        return '_<Reaction>_';

      case 'protocolMessage':
        return '_<System Message>_';

      case 'viewOnceMessageV2':
        return '_<View Once Message>_';

      case 'ephemeralMessage':
        return '_<Ephemeral Message>_';

      default:
        return '_<Message>_';
    }
  }

  public sliceIntoChunks(arr: any[], chunkSize: number) {
    return arr.splice(0, chunkSize);
  }

  public isGroup(remoteJid: string) {
    return remoteJid.includes('@g.us');
  }

  public isIgnorePhoneNumber(remoteJid: string) {
    return this.isGroup(remoteJid) || remoteJid === 'status@broadcast' || remoteJid === '0@s.whatsapp.net';
  }

  public updateMessageSourceID(messageId: string | number, sourceId: string) {
    const pgClient = postgresClient.getChatwootConnection();

    const sql = `UPDATE messages SET source_id = $1, status = 0, created_at = NOW(), updated_at = NOW() WHERE id = $2;`;

    return pgClient.query(sql, [`WAID:${sourceId}`, messageId]);
  }
}

export const chatwootImport = new ChatwootImport();
