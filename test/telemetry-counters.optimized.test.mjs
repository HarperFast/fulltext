import test from 'node:test';
import { mixedTelemetryCounterStress } from './fixtures/telemetry-counter-stress.mjs';

for (const histograms of [false, true]) {
	test(
		`mixed native counter stress conserves 10,080 operations with histograms ${histograms ? 'on' : 'off'}`,
		{ timeout: 60_000 },
		(context) => mixedTelemetryCounterStress(context, histograms),
	);
}
