import { afterAll, describe, expect } from 'vitest';
import test from 'vitest-gwt';
import {
  startTestDriver,
  stopTestDriver,
  uniqueDomain,
  uniqueWorkflowId,
  type CadenceTestDriver,
} from './harness/driver.js';

interface Ctx {
  driver: CadenceTestDriver;
  domain: string;
  workflowId: string;
  runId: string;
  statsBefore: { workflowStarts: number; activityStarts: number };
  statsAfter: { workflowStarts: number; activityStarts: number };
}

const TIMEOUT_MS = 60_000;

afterAll(async () => {
  await stopTestDriver();
});

describe('Interceptor context', () => {
  test<Ctx>('workflow invocations are observed by the interceptor chain', {
    given: {
      a_running_driver: async function (this: Ctx) {
        this.driver = await startTestDriver();
      },
      a_registered_domain: async function (this: Ctx) {
        this.domain = uniqueDomain('gwt-interceptor');
        await this.driver.ensureDomain(this.domain);
      },
      the_baseline_interceptor_counters: async function (this: Ctx) {
        this.statsBefore = await this.driver.interceptorStats();
      },
    },
    when: {
      running_one_echo_workflow_to_completion: async function (this: Ctx) {
        this.workflowId = uniqueWorkflowId('intercepted-echo');
        const started = await this.driver.startWorkflow({
          domain: this.domain,
          workflowType: 'echo',
          taskList: 'gwt-tl',
          input: ['intercepted'],
          executionStartToCloseTimeoutMs: TIMEOUT_MS,
          workflowId: this.workflowId,
        });
        this.runId = started.runId;
        await this.driver.waitForClose({
          domain: this.domain,
          workflowId: started.workflowId,
          runId: started.runId,
          timeoutMs: TIMEOUT_MS,
        });
        this.statsAfter = await this.driver.interceptorStats();
      },
    },
    then: {
      the_workflow_start_counter_increases_by_one: function (this: Ctx) {
        expect(this.statsAfter.workflowStarts - this.statsBefore.workflowStarts).toBe(1);
      },
      the_activity_counter_is_unchanged: function (this: Ctx) {
        expect(this.statsAfter.activityStarts - this.statsBefore.activityStarts).toBe(0);
      },
    },
  });

  test<Ctx>('activity invocations are observed by the interceptor chain', {
    given: {
      a_running_driver: async function (this: Ctx) {
        this.driver = await startTestDriver();
      },
      a_registered_domain: async function (this: Ctx) {
        this.domain = uniqueDomain('gwt-interceptor-activity');
        await this.driver.ensureDomain(this.domain);
      },
      the_baseline_interceptor_counters: async function (this: Ctx) {
        this.statsBefore = await this.driver.interceptorStats();
      },
    },
    when: {
      running_one_activity_workflow_to_completion: async function (this: Ctx) {
        this.workflowId = uniqueWorkflowId('intercepted-activity');
        const started = await this.driver.startWorkflow({
          domain: this.domain,
          workflowType: 'echo-via-activity',
          taskList: 'gwt-tl',
          input: ['intercepted-activity'],
          executionStartToCloseTimeoutMs: TIMEOUT_MS,
          workflowId: this.workflowId,
        });
        this.runId = started.runId;
        await this.driver.waitForClose({
          domain: this.domain,
          workflowId: started.workflowId,
          runId: started.runId,
          timeoutMs: TIMEOUT_MS,
        });
        this.statsAfter = await this.driver.interceptorStats();
      },
    },
    then: {
      the_activity_start_counter_increases_by_one: function (this: Ctx) {
        expect(this.statsAfter.activityStarts - this.statsBefore.activityStarts).toBe(1);
      },
    },
  });
});
