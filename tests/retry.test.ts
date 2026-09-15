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
  result: { status: string; result?: unknown; failure?: { type: string; message: string; reason?: string } };
}

const TIMEOUT_MS = 60_000;

afterAll(async () => {
  await stopTestDriver();
});

describe('Retry policy context', () => {
  test<Ctx>('a transient activity failure is retried until it succeeds', {
    given: {
      a_running_driver: async function (this: Ctx) {
        this.driver = await startTestDriver();
      },
      a_registered_domain: async function (this: Ctx) {
        this.domain = uniqueDomain('gwt-retry');
        await this.driver.ensureDomain(this.domain);
      },
      an_activity_that_fails_twice_then_succeeds: async function (this: Ctx) {
        this.workflowId = uniqueWorkflowId('fail-until-attempt');
        const started = await this.driver.startWorkflow({
          domain: this.domain,
          workflowType: 'fail-until-attempt',
          workflowId: this.workflowId,
          taskList: 'gwt-tl',
          input: [2],
          executionStartToCloseTimeoutMs: TIMEOUT_MS,
        });
        this.runId = started.runId;
      },
    },
    when: {
      waiting_for_the_run_to_close: async function (this: Ctx) {
        this.result = await this.driver.waitForClose({
          domain: this.domain,
          workflowId: this.workflowId,
          runId: this.runId,
          timeoutMs: TIMEOUT_MS,
        });
      },
    },
    then: {
      the_workflow_completes: function (this: Ctx) {
        expect(this.result.status).toBe('COMPLETED');
      },
      the_result_reports_the_third_attempt: function (this: Ctx) {
        expect(this.result.result).toBe(3);
      },
    },
  });

  test<Ctx>('a non-retryable error reason stops retries immediately', {
    given: {
      a_running_driver: async function (this: Ctx) {
        this.driver = await startTestDriver();
      },
      a_registered_domain: async function (this: Ctx) {
        this.domain = uniqueDomain('gwt-retry-stop');
        await this.driver.ensureDomain(this.domain);
      },
      an_activity_that_fails_with_a_non_retryable_reason: async function (this: Ctx) {
        this.workflowId = uniqueWorkflowId('non-retryable');
        const started = await this.driver.startWorkflow({
          domain: this.domain,
          workflowType: 'fail-non-retryable',
          workflowId: this.workflowId,
          taskList: 'gwt-tl',
          input: [],
          executionStartToCloseTimeoutMs: TIMEOUT_MS,
        });
        this.runId = started.runId;
      },
    },
    when: {
      waiting_for_the_run_to_close: async function (this: Ctx) {
        this.result = await this.driver.waitForClose({
          domain: this.domain,
          workflowId: this.workflowId,
          runId: this.runId,
          timeoutMs: TIMEOUT_MS,
        });
      },
    },
    then: {
      the_workflow_fails: function (this: Ctx) {
        expect(this.result.status).toBe('FAILED');
      },
      the_failure_reason_is_the_custom_reason: function (this: Ctx) {
        expect(this.result.failure?.reason).toBe('DoNotRetry');
      },
    },
  });
});
