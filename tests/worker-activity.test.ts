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

describe('Worker & Activity context', () => {
  test<Ctx>('a workflow executes an activity and returns its result', {
    given: {
      a_running_driver: async function (this: Ctx) {
        this.driver = await startTestDriver();
      },
      a_registered_domain: async function (this: Ctx) {
        this.domain = uniqueDomain('gwt-activity');
        await this.driver.ensureDomain(this.domain);
      },
      a_started_activity_workflow: async function (this: Ctx) {
        this.workflowId = uniqueWorkflowId('echo-via-activity');
        const started = await this.driver.startWorkflow({
          domain: this.domain,
          workflowType: 'echo-via-activity',
          workflowId: this.workflowId,
          taskList: 'gwt-tl',
          input: ['activity-payload'],
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
      the_close_status_is_COMPLETED: function (this: Ctx) {
        expect(this.result.status).toBe('COMPLETED');
      },
      the_result_is_the_activity_output: function (this: Ctx) {
        expect(this.result.result).toBe('activity-payload');
      },
    },
  });

  test<Ctx>('a failing activity fails the workflow with its reason', {
    given: {
      a_running_driver: async function (this: Ctx) {
        this.driver = await startTestDriver();
      },
      a_registered_domain: async function (this: Ctx) {
        this.domain = uniqueDomain('gwt-activity-failure');
        await this.driver.ensureDomain(this.domain);
      },
      a_started_non_retryable_activity_workflow: async function (this: Ctx) {
        this.workflowId = uniqueWorkflowId('fail-non-retryable');
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
      the_close_status_is_FAILED: function (this: Ctx) {
        expect(this.result.status).toBe('FAILED');
      },
      the_failure_reason_is_the_custom_error_reason: function (this: Ctx) {
        expect(this.result.failure?.reason).toBe('DoNotRetry');
      },
    },
  });

  test<Ctx>('a child workflow returns its result to the parent', {
    given: {
      a_running_driver: async function (this: Ctx) {
        this.driver = await startTestDriver();
      },
      a_registered_domain: async function (this: Ctx) {
        this.domain = uniqueDomain('gwt-child');
        await this.driver.ensureDomain(this.domain);
      },
      a_started_child_workflow: async function (this: Ctx) {
        this.workflowId = uniqueWorkflowId('child-echo');
        const started = await this.driver.startWorkflow({
          domain: this.domain,
          workflowType: 'child-echo',
          workflowId: this.workflowId,
          taskList: 'gwt-tl',
          input: ['from-child'],
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
      the_parent_completes_with_the_child_result: function (this: Ctx) {
        expect(this.result.status).toBe('COMPLETED');
        expect(this.result.result).toBe('from-child');
      },
    },
  });

  test<Ctx>('parallel activities all complete', {
    given: {
      a_running_driver: async function (this: Ctx) {
        this.driver = await startTestDriver();
      },
      a_registered_domain: async function (this: Ctx) {
        this.domain = uniqueDomain('gwt-parallel');
        await this.driver.ensureDomain(this.domain);
      },
      a_started_parallel_echo_workflow: async function (this: Ctx) {
        this.workflowId = uniqueWorkflowId('parallel-echo');
        const started = await this.driver.startWorkflow({
          domain: this.domain,
          workflowType: 'parallel-echo',
          workflowId: this.workflowId,
          taskList: 'gwt-tl',
          input: ['left', 'right'],
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
      both_activity_results_are_returned: function (this: Ctx) {
        expect(this.result.status).toBe('COMPLETED');
        expect(this.result.result).toEqual(['left', 'right']);
      },
    },
  });
});
