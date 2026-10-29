export async function closeIndex(index, options) {
	if (!index) return;
	const result = await index.close(options);
	if (result.cleanupError) throw result.cleanupError;
}
