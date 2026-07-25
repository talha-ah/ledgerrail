/// <reference types="vitest" />

import { defineConfig, configDefaults } from "vitest/config"

export default defineConfig({
  test: {
    globals: true,
    environment: "node",
    hookTimeout: 60_000,
    fileParallelism: false,
    exclude: [...configDefaults.exclude],
    globalSetup: "./test/globalSetup.ts",
    coverage: {
      provider: "v8",
      reporter: ["text", "json", "html"],
    },
  },
})
