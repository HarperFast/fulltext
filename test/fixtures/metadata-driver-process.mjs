import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
const execute = promisify(execFile);
export async function executeDriver(context, ...args) {
	try {
		return await execute(...args);
	} catch (error) {
		context.diagnostic(error.stderr || error.message);
		throw error;
	}
}
