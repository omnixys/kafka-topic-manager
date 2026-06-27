/**
 * @license GPL-3.0-or-later
 * Copyright (C) 2025 Caleb Gyamfi - Omnixys Technologies
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details.
 *
 * For more information, visit <https://www.gnu.org/licenses/>.
 */
/** Conventional Commit rules for Omnixys ecosystem */
export default {
  extends: ["@commitlint/config-conventional"],
  formatter: "./.extras/scripts/commitlint-formatter.js",
  rules: {
    "type-enum": [
      2,
      "always",
      [
        "build",
        "chore",
        "ci",
        "docs",
        "feat",
        "fix",
        "perf",
        "refactor",
        "revert",
        "style",
        "test",
        "breaking",
      ],
    ],
    "scope-case": [2, "always", "lower-case"],
    "subject-case": [1, "always", ["sentence-case", "lower-case"]],
    "header-max-length": [2, "always", 100],

    /**
     * 🔴 REQUIRE at least one issue reference
     * Accepts:
     * - Closes: TEST-1234
     * - Refs: TEST-1234
     * - Fixes: TEST-1234
     */
    // 'references-empty': [2, 'never'],
  },
};
