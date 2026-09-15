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
  openRuns: Array<{ workflowId: string; status: string; memo?: Record<string, unknown> }>;
  closedRuns: Array<{ workflowId: string; status: string; memo?: Record<string, unknown> }>;
}

const TIMEOUT_MS = 60_000;

afterAll(async () => {
  await stopTestDriver();
});

describe('Visibility context (workflow listing)', () => {
  test<Ctx>('open executions are listed while running', {
    given: {
      a_running_driver: async function (this: Ctx) {
        this.driver = await startTestDriver();
      },
      a_registered_domain: async function (this: Ctx) {
        this.domain = uniqueDomain('gwt-visibility');
        await this.driver.ensureDomain(this.domain);
      },
      a_running_sleep_workflow: async function (this: Ctx) {
        this.workflowId = uniqueWorkflowId('visible-open');
        const started = await this.driver.startWorkflow({
          domain: this.domain,
          workflowType: 'sleep',
          taskList: 'gwt-tl',
          input: [8000],
          executionStartToCloseTimeoutMs: 30_000,
          workflowId: this.workflowId,
          memo: { test: 'visibility-open' },
        });
        this.runId = started.runId;
      },
    },
    when: {
      listing_open_executions: async function (this: Ctx) {
        this.openRuns = await this.driver.listOpenWorkflowExecutions({
          domain: this.domain,
          maxResults: 100,
        });
      },
    },
    then: {
      the_running_workflow_appears_in_the_open_list: function (this: Ctx) {
        const match = this.openRuns.find((r) => r.workflowId === this.workflowId);
        expect(match).toBeDefined();
        expect(match?.status).toBe('RUNNING');
      },
      the_memo_is_visible_on_the_listed_run: function (this: Ctx) {
        const match = this.openRuns.find((r) => r.workflowId === this.workflowId);
        expect(match?.memo?.test).toBe('visibility-open');
      },
    },
  });

  test<Ctx>('closed executions are listed with their close status', {
    given: {
      a_running_driver: async function (this: Ctx) {
        this.driver = await startTestDriver();
      },
      a_registered_domain: async function (this: Ctx) {
        this.domain = uniqueDomain('gwt-visibility-closed');
        await this.driver.ensureDomain(this.domain);
      },
      a_completed_echo_workflow: async function (this: Ctx) {
        this.workflowId = uniqueWorkflowId('visible-closed');
        const started = await this.driver.startWorkflow({
          domain: this.domain,
          workflowType: 'echo',
          taskList: 'gwt-tl',
          input: ['done'],
          executionStartToCloseTimeoutMs: TIMEOUT_MS,
          workflowId: this.workflowId,
          memo: { test: 'visibility-closed' },
        });
        this.runId = started.runId;
        await this.driver.waitForClose({
          domain: this.domain,
          workflowId: started.workflowId,
          runId: started.runId,
          timeoutMs: TIMEOUT_MS,
        });
      },
    },
    when: {
      listing_closed_executions: async function (this: Ctx) {
        this.closedRuns = await this.driver.listClosedWorkflowExecutions({
          domain: this.domain,
          maxResults: 100,
          workflowId: this.workflowId,
        });
      },
    },
    then: {
      the_completed_workflow_appears_as_COMPLETED: function (this: Ctx) {
        const match = this.closedRuns.find((r) => r.workflowId === this.workflowId);
        expect(match).toBeDefined();
        expect(match?.status).toBe('COMPLETED');
      },
      the_memo_survives_to_the_closed_list: function (this: Ctx) {
        const match = this.closedRuns.find((r) => r.workflowId === this.workflowId);
        expect(match?.memo?.test).toBe('visibility-closed');
      },
    },
  });
});
