import { RouterBroker } from '@api/abstract/abstract.router';
import { InstanceDto } from '@api/dto/instance.dto';
import { ChatwootDto } from '@api/integrations/chatbot/chatwoot/dto/chatwoot.dto';
import { HttpStatus } from '@api/routes/index.router';
import { chatwootController } from '@api/server.module';
import { chatwootSchema, instanceSchema } from '@validate/validate.schema';
import { RequestHandler, Router } from 'express';

export class ChatwootRouter extends RouterBroker {
  constructor(...guards: RequestHandler[]) {
    super();
    this.router
      .post(this.routerPath('set'), ...guards, async (req, res) => {
        const response = await this.dataValidate<ChatwootDto>({
          request: req,
          schema: chatwootSchema,
          ClassRef: ChatwootDto,
          execute: (instance, data) => chatwootController.createChatwoot(instance, data),
        });

        res.status(HttpStatus.CREATED).json(response);
      })
      .get(this.routerPath('find'), ...guards, async (req, res) => {
        const response = await this.dataValidate<InstanceDto>({
          request: req,
          schema: instanceSchema,
          ClassRef: InstanceDto,
          execute: (instance) => chatwootController.findChatwoot(instance),
        });

        res.status(HttpStatus.OK).json(response);
      })
      .post(this.routerPath('webhook'), async (req, res) => {
        const response = await this.dataValidate<InstanceDto>({
          request: req,
          schema: instanceSchema,
          ClassRef: InstanceDto,
          execute: (instance, data) => chatwootController.receiveWebhook(instance, data),
        });

        res.status(HttpStatus.OK).json(response);
      })
      .post(this.routerPath('importFromDb'), ...guards, async (req, res) => {
        const instance = { instanceName: req.params.instanceName } as InstanceDto;
        const daysLimit = req.query.daysLimit ? Number(req.query.daysLimit) : undefined;
        const batchSize = req.query.batchSize ? Math.min(10000, Math.max(1000, Number(req.query.batchSize))) : 5000;
        const response = await chatwootController.importFromEvolutionDb(instance, daysLimit, batchSize);
        res.status(HttpStatus.OK).json(response);
      });
  }

  public readonly router: Router = Router();
}
