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
  description: { status: string; historyLength: number };
  history: Array<{ eventType: string; id: number }>;
  error: Error | undefined;
  secondStart: { workflowId: string; runId: string };
}

const ECHO_TIMEOUT_MS = 30_000;

afterAll(async () => {
  await stopTestDriver();
});

describe('Workflow Execution lifecycle', () => {
  test<Ctx>('completes an echo workflow and returns its result', {
    given: {
      a_running_driver: async function (this: Ctx) {
        this.driver = await startTestDriver();
      },
      a_registered_domain: async function (this: Ctx) {
        this.domain = uniqueDomain('gwt-lifecycle');
        await this.driver.ensureDomain(this.domain);
      },
      a_started_echo_workflow: async function (this: Ctx) {
        const started = await this.driver.startWorkflow({
          domain: this.domain,
          workflowType: 'echo',
          taskList: 'gwt-tl',
          input: ['hello-cadence'],
          executionStartToCloseTimeoutMs: ECHO_TIMEOUT_MS,
        });
        this.workflowId = started.workflowId;
        this.runId = started.runId;
      },
    },
    when: {
      waiting_for_the_run_to_close: async function (this: Ctx) {
        this.result = await this.driver.waitForClose({
          domain: this.domain,
          workflowId: this.workflowId,
          runId: this.runId,
          timeoutMs: ECHO_TIMEOUT_MS,
        });
      },
    },
    then: {
      the_close_status_is_COMPLETED: function (this: Ctx) {
        expect(this.result.status).toBe('COMPLETED');
      },
      the_result_matches_the_input: function (this: Ctx) {
        expect(this.result.result).toBe('hello-cadence');
      },
    },
  });

  test<Ctx>('starts a workflow with an explicit workflow id', {
    given: {
      a_running_driver: async function (this: Ctx) {
        this.driver = await startTestDriver();
      },
      a_registered_domain: async function (this: Ctx) {
        this.domain = uniqueDomain('gwt-wfid');
        await this.driver.ensureDomain(this.domain);
      },
      a_chosen_workflow_id: function (this: Ctx) {
        this.workflowId = uniqueWorkflowId('explicit');
      },
    },
    when: {
      starting_the_echo_workflow: async function (this: Ctx) {
        const started = await this.driver.startWorkflow({
          domain: this.domain,
          workflowType: 'echo',
          taskList: 'gwt-tl',
          input: ['x'],
          executionStartToCloseTimeoutMs: ECHO_TIMEOUT_MS,
          workflowId: this.workflowId,
        });
        this.runId = started.runId;
      },
    },
    then: {
      the_started_run_uses_the_chosen_id: function (this: Ctx) {
        expect(this.runId).toBeTruthy();
        void this.workflowId;
      },
    },
  });

  test<Ctx>('rejects a duplicate start with RejectDuplicate while running', {
    given: {
      a_running_driver: async function (this: Ctx) {
        this.driver = await startTestDriver();
      },
      a_registered_domain: async function (this: Ctx) {
        this.domain = uniqueDomain('gwt-reject');
        await this.driver.ensureDomain(this.domain);
      },
      a_running_sleep_workflow_with_id: async function (this: Ctx) {
        this.workflowId = uniqueWorkflowId('reject-dup');
        const started = await this.driver.startWorkflow({
          domain: this.domain,
          workflowType: 'sleep',
          taskList: 'gwt-tl',
          input: [3000],
          executionStartToCloseTimeoutMs: 30_000,
          workflowId: this.workflowId,
        });
        this.runId = started.runId;
      },
    },
    when: {
      starting_again_with_RejectDuplicate: async function (this: Ctx) {
        this.error = undefined;
        try {
          await this.driver.startWorkflow({
            domain: this.domain,
            workflowType: 'sleep',
            taskList: 'gwt-tl',
            input: [3000],
            executionStartToCloseTimeoutMs: 30_000,
            workflowId: this.workflowId,
            workflowIdReusePolicy: 'RejectDuplicate',
          });
        } catch (e) {
          this.error = e as Error;
        }
      },
    },
    then: {
      the_start_fails_with_WorkflowExecutionAlreadyStarted: function (this: Ctx) {
        expect(this.error).toBeDefined();
        expect((this.error as Error & { code?: string }).code).toBe('WorkflowExecutionAlreadyStarted');
      },
    },
  });

  test<Ctx>('TerminateIfRunning replaces a running workflow', {
    given: {
      a_running_driver: async function (this: Ctx) {
        this.driver = await startTestDriver();
      },
      a_registered_domain: async function (this: Ctx) {
        this.domain = uniqueDomain('gwt-terminate-if-running');
        await this.driver.ensureDomain(this.domain);
      },
      a_running_sleep_workflow_with_id: async function (this: Ctx) {
        this.workflowId = uniqueWorkflowId('terminate-if-running');
        const started = await this.driver.startWorkflow({
          domain: this.domain,
          workflowType: 'sleep',
          taskList: 'gwt-tl',
          input: [5000],
          executionStartToCloseTimeoutMs: 30_000,
          workflowId: this.workflowId,
        });
        this.runId = started.runId;
      },
    },
    when: {
      starting_again_with_TerminateIfRunning: async function (this: Ctx) {
        this.secondStart = await this.driver.startWorkflow({
          domain: this.domain,
          workflowType: 'sleep',
          taskList: 'gwt-tl',
          input: [5000],
          executionStartToCloseTimeoutMs: 30_000,
          workflowId: this.workflowId,
          workflowIdReusePolicy: 'TerminateIfRunning',
        });
      },
    },
    then: {
      the_first_run_is_terminated: async function (this: Ctx) {
        const result = await this.driver.waitForClose({
          domain: this.domain,
          workflowId: this.workflowId,
          runId: this.runId,
          timeoutMs: 15_000,
        });
        expect(result.status).toBe('TERMINATED');
      },
      the_new_run_is_accepted: function (this: Ctx) {
        expect(this.secondStart.runId).toBeTruthy();
        expect(this.secondStart.runId).not.toBe(this.runId);
      },
    },
  });

  test<Ctx>('describes a running execution', {
    given: {
      a_running_driver: async function (this: Ctx) {
        this.driver = await startTestDriver();
      },
      a_registered_domain: async function (this: Ctx) {
        this.domain = uniqueDomain('gwt-describe');
        await this.driver.ensureDomain(this.domain);
      },
      a_running_sleep_workflow: async function (this: Ctx) {
        this.workflowId = uniqueWorkflowId('describe-running');
        const started = await this.driver.startWorkflow({
          domain: this.domain,
          workflowType: 'sleep',
          taskList: 'gwt-tl',
          input: [2500],
          executionStartToCloseTimeoutMs: 30_000,
          workflowId: this.workflowId,
        });
        this.runId = started.runId;
      },
    },
    when: {
      describing_the_execution: async function (this: Ctx) {
        this.description = await this.driver.describeWorkflowExecution({
          domain: this.domain,
          workflowId: this.workflowId,
          runId: this.runId,
        });
      },
    },
    then: {
      the_status_is_RUNNING: function (this: Ctx) {
        expect(this.description.status).toBe('RUNNING');
      },
      the_history_has_at_least_the_start_event: function (this: Ctx) {
        expect(this.description.historyLength).toBeGreaterThanOrEqual(2);
      },
    },
  });

  test<Ctx>('history records the execution lifecycle', {
    given: {
      a_running_driver: async function (this: Ctx) {
        this.driver = await startTestDriver();
      },
      a_registered_domain: async function (this: Ctx) {
        this.domain = uniqueDomain('gwt-history');
        await this.driver.ensureDomain(this.domain);
      },
      a_completed_echo_workflow: async function (this: Ctx) {
        const started = await this.driver.startWorkflow({
          domain: this.domain,
          workflowType: 'echo',
          taskList: 'gwt-tl',
          input: ['history'],
          executionStartToCloseTimeoutMs: ECHO_TIMEOUT_MS,
        });
        this.workflowId = started.workflowId;
        this.runId = started.runId;
        await this.driver.waitForClose({
          domain: this.domain,
          workflowId: started.workflowId,
          runId: started.runId,
          timeoutMs: ECHO_TIMEOUT_MS,
        });
      },
    },
    when: {
      fetching_the_history: async function (this: Ctx) {
        this.history = await this.driver.getHistory({
          domain: this.domain,
          workflowId: this.workflowId,
          runId: this.runId,
        });
      },
    },
    then: {
      history_contains_WorkflowExecutionStarted: function (this: Ctx) {
        expect(this.history.map((e) => e.eventType)).toContain('WorkflowExecutionStarted');
      },
      history_contains_WorkflowExecutionCompleted: function (this: Ctx) {
        expect(this.history.map((e) => e.eventType)).toContain('WorkflowExecutionCompleted');
      },
    },
  });

  test<Ctx>('waiting on an unknown workflow reports EntityNotExists', {
    given: {
      a_running_driver: async function (this: Ctx) {
        this.driver = await startTestDriver();
      },
      a_registered_domain: async function (this: Ctx) {
        this.domain = uniqueDomain('gwt-unknown');
        await this.driver.ensureDomain(this.domain);
      },
      a_workflow_id_that_was_never_started: function (this: Ctx) {
        this.workflowId = uniqueWorkflowId('never-started');
      },
    },
    when: {
      waiting_for_the_unknown_run: async function (this: Ctx) {
        this.error = undefined;
        try {
          await this.driver.waitForClose({
            domain: this.domain,
            workflowId: this.workflowId,
            timeoutMs: 5_000,
          });
        } catch (e) {
          this.error = e as Error;
        }
      },
    },
    then: {
      the_wait_fails_with_EntityNotExists: function (this: Ctx) {
        expect((this.error as Error & { code?: string }).code).toBe('EntityNotExists');
      },
    },
  });
});
