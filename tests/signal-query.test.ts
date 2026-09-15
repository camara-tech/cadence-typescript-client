import { afterAll, describe, expect } from 'vitest';
import test from 'vitest-gwt';
import {
  startTestDriver,
  stopTestDriver,
  uniqueDomain,
  uniqueWorkflowId,
  type CadenceTestDriver,
} from './harness/driver.js';
import { queryUntil } from './harness/helpers.js';

interface Ctx {
  driver: CadenceTestDriver;
  domain: string;
  workflowId: string;
  runId: string;
  queryValue: unknown;
  error: Error | undefined;
  signalWithStart: { workflowId: string; runId: string };
  secondSignalWithStart: { workflowId: string; runId: string };
  result: { status: string; result?: unknown; failure?: { type: string; message: string; reason?: string } };
}

const TIMEOUT_MS = 60_000;

afterAll(async () => {
  await stopTestDriver();
});

describe('Signal & Query context', () => {
  test<Ctx>('queries the initial state of a running workflow', {
    given: {
      a_running_driver: async function (this: Ctx) {
        this.driver = await startTestDriver();
      },
      a_registered_domain: async function (this: Ctx) {
        this.domain = uniqueDomain('gwt-signal-query');
        await this.driver.ensureDomain(this.domain);
      },
      a_running_signal_counter: async function (this: Ctx) {
        this.workflowId = uniqueWorkflowId('query-initial');
        const started = await this.driver.startWorkflow({
          domain: this.domain,
          workflowType: 'signal-counter',
          workflowId: this.workflowId,
          taskList: 'gwt-tl',
          input: ['done'],
          executionStartToCloseTimeoutMs: TIMEOUT_MS,
        });
        this.runId = started.runId;
      },
    },
    when: {
      querying_the_count: async function (this: Ctx) {
        this.queryValue = await queryUntil(this.driver, {
          domain: this.domain,
          workflowId: this.workflowId,
          runId: this.runId,
          queryType: 'count',
        });
      },
    },
    then: {
      the_count_is_zero: function (this: Ctx) {
        expect(this.queryValue).toBe(0);
      },
    },
  });

  test<Ctx>('a signal updates state visible to queries', {
    given: {
      a_running_driver: async function (this: Ctx) {
        this.driver = await startTestDriver();
      },
      a_registered_domain: async function (this: Ctx) {
        this.domain = uniqueDomain('gwt-signal-update');
        await this.driver.ensureDomain(this.domain);
      },
      a_running_signal_counter: async function (this: Ctx) {
        this.workflowId = uniqueWorkflowId('signal-update');
        const started = await this.driver.startWorkflow({
          domain: this.domain,
          workflowType: 'signal-counter',
          workflowId: this.workflowId,
          taskList: 'gwt-tl',
          input: ['done'],
          executionStartToCloseTimeoutMs: TIMEOUT_MS,
        });
        this.runId = started.runId;
      },
      an_increment_signal_of_five: async function (this: Ctx) {
        await this.driver.signalWorkflow({
          domain: this.domain,
          workflowId: this.workflowId,
          signalName: 'increment',
          input: 5,
        });
      },
    },
    when: {
      querying_the_count: async function (this: Ctx) {
        this.queryValue = await queryUntil(this.driver, {
          domain: this.domain,
          workflowId: this.workflowId,
          runId: this.runId,
          queryType: 'count',
        });
      },
    },
    then: {
      the_count_reflects_the_signal: function (this: Ctx) {
        expect(this.queryValue).toBe(5);
      },
    },
  });

  test<Ctx>('the finish signal completes the workflow with the total', {
    given: {
      a_running_driver: async function (this: Ctx) {
        this.driver = await startTestDriver();
      },
      a_registered_domain: async function (this: Ctx) {
        this.domain = uniqueDomain('gwt-signal-finish');
        await this.driver.ensureDomain(this.domain);
      },
      a_running_signal_counter: async function (this: Ctx) {
        this.workflowId = uniqueWorkflowId('signal-finish');
        const started = await this.driver.startWorkflow({
          domain: this.domain,
          workflowType: 'signal-counter',
          workflowId: this.workflowId,
          taskList: 'gwt-tl',
          input: ['done'],
          executionStartToCloseTimeoutMs: TIMEOUT_MS,
        });
        this.runId = started.runId;
      },
      two_increment_signals: async function (this: Ctx) {
        await this.driver.signalWorkflow({
          domain: this.domain,
          workflowId: this.workflowId,
          signalName: 'increment',
          input: 2,
        });
        await this.driver.signalWorkflow({
          domain: this.domain,
          workflowId: this.workflowId,
          signalName: 'increment',
          input: 3,
        });
      },
      the_finish_signal: async function (this: Ctx) {
        await this.driver.signalWorkflow({
          domain: this.domain,
          workflowId: this.workflowId,
          runId: this.runId,
          signalName: 'done',
        });
      },
    },
    when: {
      waiting_for_the_run_to_close: async function (this: Ctx) {
        this.result = await this.driver.waitForClose({
          domain: this.domain,
          workflowId: this.workflowId,
          runId: this.runId,
          timeoutMs: 30_000,
        });
      },
    },
    then: {
      the_close_status_is_COMPLETED: function (this: Ctx) {
        expect(this.result.status).toBe('COMPLETED');
      },
      the_result_is_the_sum_of_signals: function (this: Ctx) {
        expect(this.result.result).toBe(5);
      },
    },
  });

  test<Ctx>('signalWithStart starts the workflow when none is running', {
    given: {
      a_running_driver: async function (this: Ctx) {
        this.driver = await startTestDriver();
      },
      a_registered_domain: async function (this: Ctx) {
        this.domain = uniqueDomain('gwt-signal-with-start');
        await this.driver.ensureDomain(this.domain);
      },
      a_chosen_workflow_id: function (this: Ctx) {
        this.workflowId = uniqueWorkflowId('sws');
      },
    },
    when: {
      signaling_with_start: async function (this: Ctx) {
        this.signalWithStart = await this.driver.signalWithStartWorkflow({
          domain: this.domain,
          workflowId: this.workflowId,
          signalName: 'increment',
          signalInput: 2,
          workflowType: 'signal-counter',
          taskList: 'gwt-tl',
          input: ['done'],
          executionStartToCloseTimeoutMs: TIMEOUT_MS,
        });
        this.runId = this.signalWithStart.runId;
      },
    },
    then: {
      the_workflow_was_started_with_the_chosen_id: function (this: Ctx) {
        expect(this.signalWithStart.workflowId).toBe(this.workflowId);
        expect(this.signalWithStart.runId).toBeTruthy();
      },
      the_signal_was_delivered_to_the_new_run: async function (this: Ctx) {
        const value = await queryUntil(this.driver, {
          domain: this.domain,
          workflowId: this.workflowId,
          runId: this.runId,
          queryType: 'count',
        });
        expect(value).toBe(2);
      },
    },
  });

  test<Ctx>('signalWithStart on a running workflow reuses the running run', {
    given: {
      a_running_driver: async function (this: Ctx) {
        this.driver = await startTestDriver();
      },
      a_registered_domain: async function (this: Ctx) {
        this.domain = uniqueDomain('gwt-sws-running');
        await this.driver.ensureDomain(this.domain);
      },
      a_workflow_started_via_signalWithStart: async function (this: Ctx) {
        this.workflowId = uniqueWorkflowId('sws-running');
        this.signalWithStart = await this.driver.signalWithStartWorkflow({
          domain: this.domain,
          workflowId: this.workflowId,
          signalName: 'increment',
          signalInput: 1,
          workflowType: 'signal-counter',
          taskList: 'gwt-tl',
          input: ['done'],
          executionStartToCloseTimeoutMs: TIMEOUT_MS,
        });
        this.runId = this.signalWithStart.runId;
      },
    },
    when: {
      signaling_with_start_again: async function (this: Ctx) {
        this.secondSignalWithStart = await this.driver.signalWithStartWorkflow({
          domain: this.domain,
          workflowId: this.workflowId,
          signalName: 'increment',
          signalInput: 3,
          workflowType: 'signal-counter',
          taskList: 'gwt-tl',
          input: ['done'],
          executionStartToCloseTimeoutMs: TIMEOUT_MS,
        });
      },
    },
    then: {
      the_same_run_is_reused: function (this: Ctx) {
        expect(this.secondSignalWithStart.runId).toBe(this.signalWithStart.runId);
      },
      the_signal_reached_the_running_run: async function (this: Ctx) {
        const value = await queryUntil(this.driver, {
          domain: this.domain,
          workflowId: this.workflowId,
          runId: this.runId,
          queryType: 'count',
        });
        expect(value).toBe(4);
      },
    },
  });

  test<Ctx>('querying an unknown workflow reports EntityNotExists', {
    given: {
      a_running_driver: async function (this: Ctx) {
        this.driver = await startTestDriver();
      },
      a_registered_domain: async function (this: Ctx) {
        this.domain = uniqueDomain('gwt-query-unknown');
        await this.driver.ensureDomain(this.domain);
      },
      a_workflow_id_that_was_never_started: function (this: Ctx) {
        this.workflowId = uniqueWorkflowId('never-started-query');
      },
    },
    when: {
      querying_the_unknown_workflow: async function (this: Ctx) {
        this.error = undefined;
        try {
          await this.driver.queryWorkflow({
            domain: this.domain,
            workflowId: this.workflowId,
            queryType: 'count',
          });
        } catch (e) {
          this.error = e as Error;
        }
      },
    },
    then: {
      the_query_fails_with_EntityNotExists: function (this: Ctx) {
        expect((this.error as Error & { code?: string }).code).toBe('EntityNotExists');
      },
    },
  });
});
