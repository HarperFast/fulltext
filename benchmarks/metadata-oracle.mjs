import assert from 'node:assert';

export function prepareOracle(scenario) {
	const sorts = scenario.request.sort
		? Array.isArray(scenario.request.sort)
			? scenario.request.sort
			: [scenario.request.sort]
		: [];
	const ordered = [...scenario.selected].sort((a, b) => {
		for (const sort of sorts) {
			const cmp =
				typeof a[sort.field] === 'number'
					? a[sort.field] - b[sort.field]
					: Buffer.compare(Buffer.from(a[sort.field]), Buffer.from(b[sort.field]));
			if (cmp) return sort.direction === 'asc' ? cmp : -cmp;
		}
		return Buffer.compare(Buffer.from(a.id), Buffer.from(b.id));
	});
	scenario.expected = {
		total: scenario.selected.length,
		ids: ordered.slice(0, 10).map((row) => row.id),
		aggregations: (scenario.request.aggregations ?? []).map((request) => oracleAggregation(request, scenario.selected)),
	};
}
export function validate(result, scenario) {
	assert.strictEqual(result.total, scenario.expected.total);
	assert.strictEqual(result.totalRelation, 'exact');
	assert.deepStrictEqual(
		result.hits.map((hit) => hit.id),
		scenario.expected.ids,
	);
	assert.deepStrictEqual(canonical(result.aggregations ?? []), canonical(scenario.expected.aggregations));
}
function canonical(results) {
	return results.map((result) => {
		if (!result.buckets) return result;
		const buckets = result.buckets.map((bucket) => ({
			...bucket,
			...(bucket.aggregations ? { aggregations: canonical(bucket.aggregations) } : {}),
		}));
		if (result.type === 'range') for (const bucket of buckets) delete bucket.key;
		if (result.type === 'terms') buckets.sort((a, b) => Buffer.compare(Buffer.from(a.key), Buffer.from(b.key)));
		return { ...result, buckets };
	});
}
function oracleAggregation(request, selected) {
	const base = { name: request.name, type: request.type };
	if (request.type === 'stats') {
		const values = selected.map((row) => row[request.field]);
		const sum = values.reduce((a, b) => a + b, 0);
		return {
			...base,
			count: values.length,
			sum,
			min: values.length ? values.reduce((a, b) => Math.min(a, b), Infinity) : null,
			max: values.length ? values.reduce((a, b) => Math.max(a, b), -Infinity) : null,
			avg: values.length ? sum / values.length : null,
		};
	}
	const children = (rows) =>
		request.aggregations?.length
			? { aggregations: request.aggregations.map((child) => oracleAggregation(child, rows)) }
			: {};
	if (request.type === 'terms') {
		const groups = new Map();
		for (const row of selected) {
			const key = row[request.field];
			if (!groups.has(key)) groups.set(key, []);
			groups.get(key).push(row);
		}
		return {
			...base,
			buckets: [...groups].map(([key, rows]) => ({ key, count: rows.length, ...children(rows) })),
			sumOtherDocCount: 0,
			docCountErrorUpperBound: 0,
		};
	}
	if (request.type === 'histogram') {
		assert(
			Number.isFinite(request.interval) && request.interval >= 1,
			'histogram oracle requires a finite interval >= 1',
		);
		const groups = new Map();
		for (const row of selected) {
			const value = row[request.field];
			assert(Number.isSafeInteger(value), 'histogram oracle requires scalar safe integer values');
			const key = Math.floor(value / request.interval) * request.interval;
			if (!groups.has(key)) groups.set(key, []);
			groups.get(key).push(row);
		}
		return {
			...base,
			buckets: [...groups]
				.sort(([left], [right]) => left - right)
				.map(([key, rows]) => ({ key, count: rows.length, ...children(rows) })),
		};
	}
	if (request.type !== 'range') throw new Error(`Unsupported oracle aggregation type: ${request.type}`);
	return {
		...base,
		buckets: request.ranges.map((range) => {
			const rows = selected.filter(
				(row) =>
					(range.from === undefined || row[request.field] >= range.from) &&
					(range.to === undefined || row[request.field] < range.to),
			);
			return { key: `${range.from ?? '*'}-${range.to ?? '*'}`, ...range, count: rows.length, ...children(rows) };
		}),
	};
}
