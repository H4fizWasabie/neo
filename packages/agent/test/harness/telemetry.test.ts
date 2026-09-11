import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { createTypedSpanStarter, NOOP_TELEMETRY_CONTEXT, type TelemetryContext } from "theoses-telemetry";
import { describe, expect, expectTypeOf, it } from "vitest";
import { renderAgentTelemetrySchemaMarkdown } from "../../scripts/generate-telemetry-docs.ts";
import {
	AGENT_TELEMETRY_SCHEMAS,
	AI_TELEMETRY_SCHEMA,
	type AiSpanEndAttributes,
	type AiSpanStartAttributes,
	HARNESS_TELEMETRY_SCHEMA,
	type HarnessSpanEndAttributes,
	type HarnessSpanStartAttributes,
	startAiSpan,
	startHarnessSpan,
} from "../../src/harness/telemetry.ts";

describe("agent telemetry schemas", () => {
	it("serializes both schemas and generates the checked-in reference", () => {
		expect(() => JSON.stringify(AI_TELEMETRY_SCHEMA)).not.toThrow();
		expect(() => JSON.stringify(HARNESS_TELEMETRY_SCHEMA)).not.toThrow();
		expect(AGENT_TELEMETRY_SCHEMAS).toEqual([AI_TELEMETRY_SCHEMA, HARNESS_TELEMETRY_SCHEMA]);
		expect(Object.keys(HARNESS_TELEMETRY_SCHEMA.spans)).toEqual([
			"theoses.harness.run",
			"theoses.harness.compaction",
			"theoses.harness.navigation",
			"theoses.harness.checkpoint",
			"theoses.harness.turn",
			"theoses.harness.step",
			"theoses.harness.tool",
			"theoses.harness.hook",
			"theoses.harness.sleep",
			"theoses.harness.event_handler",
			"theoses.session.write",
		]);
		const actual = readFileSync(resolve(import.meta.dirname, "../../docs/telemetry-schema.md"), "utf8");
		expect(actual).toBe(renderAgentTelemetrySchemaMarkdown());
	});

	it("starts AI-request and harness spans through one composed typed starter", async () => {
		const startSpan = createTypedSpanStarter(NOOP_TELEMETRY_CONTEXT, AGENT_TELEMETRY_SCHEMAS);
		await startSpan(
			"theoses.harness.step",
			{
				"theoses.lane.name": "main",
				"theoses.operation.id": "operation",
				"theoses.step.kind": "assistant",
				"theoses.step.attempt": 1,
			},
			async (stepSpan, startChildSpan) => {
				stepSpan.setAttributes({ "theoses.step.outcome": "succeeded" });
				await startChildSpan(
					"theoses.ai.request",
					{
						"theoses.ai.operation": "stream",
						"theoses.ai.provider": "provider",
						"theoses.ai.model": "model",
						"theoses.ai.api": "api",
						"theoses.ai.streaming": true,
					},
					(requestSpan) => {
						requestSpan.setAttributes({ "theoses.ai.response.stop_reason": "stop" });
					},
				);
			},
		);
	});

	it("infers exact AI start and optional end attributes", async () => {
		type Start = AiSpanStartAttributes<"theoses.ai.request">;
		type End = AiSpanEndAttributes<"theoses.ai.request">;
		expectTypeOf<Start>().toMatchTypeOf<{
			"theoses.ai.operation": "stream" | "fetch_deferred" | "cancel_deferred" | "generate_images";
			"theoses.ai.provider": string;
			"theoses.ai.model": string;
			"theoses.ai.api": string;
			"theoses.ai.streaming": boolean;
			"theoses.ai.deferred"?: boolean;
		}>();
		expectTypeOf<End["theoses.ai.response.stop_reason"]>().toEqualTypeOf<
			"stop" | "length" | "tool_use" | "error" | "aborted" | "deferred" | undefined
		>();

		const telemetryContext: TelemetryContext = NOOP_TELEMETRY_CONTEXT;
		await startAiSpan(
			telemetryContext,
			"theoses.ai.request",
			{
				"theoses.ai.operation": "stream",
				"theoses.ai.provider": "provider",
				"theoses.ai.model": "model",
				"theoses.ai.api": "api",
				"theoses.ai.streaming": true,
			},
			(span) => {
				span.setAttributes({ "theoses.ai.response.stop_reason": "tool_use" });
				// @ts-expect-error pi.ai.request declares no span events
				span.addEvent("chunk");
			},
		);

		const compileTimeFailures = () => {
			const extraAttributes = {
				"theoses.ai.operation": "stream",
				"theoses.ai.provider": "provider",
				"theoses.ai.model": "model",
				"theoses.ai.api": "api",
				"theoses.ai.streaming": true,
				"theoses.ai.unknown": true,
			} as const;
			// @ts-expect-error variables with unknown attributes are rejected
			void startAiSpan(telemetryContext, "theoses.ai.request", extraAttributes, () => {});
			// @ts-expect-error missing required start attributes
			void startAiSpan(telemetryContext, "theoses.ai.request", { "theoses.ai.operation": "stream" }, () => {});
		};
		expectTypeOf(compileTimeFailures).toBeFunction();
	});

	it("infers per-span harness literals and optional completion enrichment", async () => {
		type RunStart = HarnessSpanStartAttributes<"theoses.harness.run">;
		type RunEnd = HarnessSpanEndAttributes<"theoses.harness.run">;
		expectTypeOf<RunStart["theoses.operation.kind"]>().toEqualTypeOf<"run">();
		expectTypeOf<RunEnd["theoses.operation.outcome"]>().toEqualTypeOf<
			"completed" | "aborted" | "failed" | "suspended" | undefined
		>();

		const telemetryContext: TelemetryContext = NOOP_TELEMETRY_CONTEXT;
		await startHarnessSpan(
			telemetryContext,
			"theoses.harness.run",
			{
				"theoses.session.id": "session",
				"theoses.lane.name": "main",
				"theoses.operation.id": "operation",
				"theoses.operation.kind": "run",
				"theoses.operation.recovery": false,
			},
			(span) => {
				span.setAttributes({ "theoses.operation.outcome": "completed" });
				span.setAttributes({});
				// @ts-expect-error the harness schema declares no span events
				span.addEvent("result");
			},
		);

		const compileTimeFailures = () => {
			const extraRunAttributes = {
				"theoses.session.id": "session",
				"theoses.lane.name": "main",
				"theoses.operation.id": "operation",
				"theoses.operation.kind": "run",
				"theoses.operation.recovery": false,
				"theoses.unknown": true,
			} as const;
			// @ts-expect-error variables with unknown attributes are rejected
			void startHarnessSpan(telemetryContext, "theoses.harness.run", extraRunAttributes, () => {});
			void startHarnessSpan(
				telemetryContext,
				"theoses.harness.checkpoint",
				{
					"theoses.lane.name": "main",
					"theoses.operation.id": "operation",
					"theoses.checkpoint.kind": "normal",
				},
				(span) => {
					// @ts-expect-error empty end schemas reject every attribute
					span.setAttributes({ "theoses.unknown": true });
				},
			);
			void startHarnessSpan(
				telemetryContext,
				"theoses.harness.run",
				{
					"theoses.session.id": "session",
					"theoses.lane.name": "main",
					"theoses.operation.id": "operation",
					// @ts-expect-error run spans accept only the run operation kind
					"theoses.operation.kind": "navigation",
					"theoses.operation.recovery": false,
				},
				() => {},
			);
			// @ts-expect-error missing required run start attributes
			void startHarnessSpan(telemetryContext, "theoses.harness.run", {}, () => {});
		};
		expectTypeOf(compileTimeFailures).toBeFunction();
	});
});
