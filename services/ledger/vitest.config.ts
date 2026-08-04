/// <reference types="vitest" />

import { defineConfig, configDefaults } from "vitest/config"

export default defineConfig({
  test: {
    globals: true,
    environment: "node",
    hookTimeout: 60_000,
    fileParallelism: false,
    // this runs inside each worker - tests run in workers
    // so, it can run multiple times depending on the number of workers
    setupFiles: "./test/setup.ts",
    exclude: [...configDefaults.exclude],
    globalSetup: "./test/globalSetup.ts", // this runs in the main process, run once
    coverage: {
      provider: "v8",
      reporter: ["text", "json", "html"],
    },
  },
})
