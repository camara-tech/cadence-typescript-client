import { afterAll, describe, expect } from 'vitest';
import test from 'vitest-gwt';
import { WorkflowClient } from '../src/client.js';
import { Connection } from '../src/connection.js';
import {
  DeleteDomainRequest,
  DeprecateDomainRequest,
  DomainAPIServiceName,
} from '../src/generated/uber/cadence/api/v1/service_domain.js';
import { startTestDriver, stopTestDriver, uniqueDomain, type CadenceTestDriver } from './harness/driver.js';

interface Ctx {
  driver: CadenceTestDriver;
  client: WorkflowClient;
  domain: string;
  otherDomain: string;
  requests: Array<{ service: string; method: string; data: Uint8Array }>;
}

afterAll(async () => {
  await stopTestDriver();
});

describe('Domain context (domain administration)', () => {
  test<Ctx>('registers a new domain idempotently', {
    given: {
      a_running_driver: async function (this: Ctx) {
        this.driver = await startTestDriver();
      },
      a_unique_domain_name: function (this: Ctx) {
        this.domain = uniqueDomain('gwt-domain');
      },
    },
    when: {
      ensuring_the_domain_exists: async function (this: Ctx) {
        await this.driver.ensureDomain(this.domain);
      },
    },
    then: {
      ensuring_again_succeeds_without_error: async function (this: Ctx) {
        await this.driver.ensureDomain(this.domain);
      },
    },
  });

  test<Ctx>('registers two distinct domains independently', {
    given: {
      a_running_driver: async function (this: Ctx) {
        this.driver = await startTestDriver();
      },
      two_unique_domain_names: function (this: Ctx) {
        this.domain = uniqueDomain('gwt-alpha');
        this.otherDomain = uniqueDomain('gwt-beta');
      },
    },
    when: {
      both_domains_are_ensured: async function (this: Ctx) {
        await this.driver.ensureDomain(this.domain);
        await this.driver.ensureDomain(this.otherDomain);
      },
    },
    then: {
      both_registrations_succeeded: function (this: Ctx) {
        // reaching this point means no error was thrown for either domain
        if (!this.domain || !this.otherDomain) throw new Error('domains were not registered');
      },
    },
  });

  test<Ctx>('deprecates a domain through the DomainAPI', {
    given: {
      a_client_with_a_recording_connection: function (this: Ctx) {
        this.requests = [];
        const connection = {
          identity: 'domain-test',
          request: async (service: string, method: string, data: Uint8Array) => {
            this.requests.push({ service, method, data });
            return new Uint8Array();
          },
        } as unknown as Connection;
        this.client = new WorkflowClient(connection);
      },
    },
    when: {
      deprecating_a_named_domain: async function (this: Ctx) {
        await this.client.deprecateDomain('retiring-domain');
      },
    },
    then: {
      the_deprecation_rpc_contains_the_domain_name: function (this: Ctx) {
        expect(this.requests).toHaveLength(1);
        expect(this.requests[0].service).toBe(DomainAPIServiceName);
        expect(this.requests[0].method).toBe('DeprecateDomain');
        expect(DeprecateDomainRequest.decode(this.requests[0].data)).toEqual({
          securityToken: '',
          name: 'retiring-domain',
        });
      },
    },
  });

  test<Ctx>('deletes a domain through the DomainAPI', {
    given: {
      a_client_with_a_recording_connection: function (this: Ctx) {
        this.requests = [];
        const connection = {
          identity: 'domain-test',
          request: async (service: string, method: string, data: Uint8Array) => {
            this.requests.push({ service, method, data });
            return new Uint8Array();
          },
        } as unknown as Connection;
        this.client = new WorkflowClient(connection);
      },
    },
    when: {
      deleting_a_named_domain: async function (this: Ctx) {
        await this.client.deleteDomain('retired-domain');
      },
    },
    then: {
      the_deletion_rpc_contains_the_domain_name: function (this: Ctx) {
        expect(this.requests).toHaveLength(1);
        expect(this.requests[0].service).toBe(DomainAPIServiceName);
        expect(this.requests[0].method).toBe('DeleteDomain');
        expect(DeleteDomainRequest.decode(this.requests[0].data)).toEqual({
          securityToken: '',
          name: 'retired-domain',
        });
      },
    },
  });
});
