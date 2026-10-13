import * as path from 'node:path'
import { defineConfig } from 'vitest/config'

// One project per package.
const project = (name: string) => ({
  root: path.join(import.meta.dirname, 'packages', name),
  test: { name, include: ['test/**/*.test.ts'], exclude: ['**/node_modules/**', '**/.wrangler/**', '**/.tsbuild/**'] },
})

export default defineConfig({
  test: { projects: [project('envelope'), project('worker')] },
})
