import { afterAll, describe } from 'vitest';
import test from 'vitest-gwt';
import { startTestDriver, stopTestDriver, uniqueDomain, type CadenceTestDriver } from './harness/driver.js';

interface Ctx {
  driver: CadenceTestDriver;
  domain: string;
  otherDomain: string;
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
});
