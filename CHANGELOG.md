# Changelog

All notable changes to this project will be documented in this file.

## [0.4.2] - 2026-02-05

### 🐛 Bug Fixes

- infer json_agg/jsonb_agg results as JSONB, not JSONB_ARRAY ([90e05bd](https://github.com/ONREZA/prisma-adapter-bun/commit/90e05bdccde4cc1577a631cb95e6d8a4b246c0d2)) by Ivan Bobchenkov

### 👷 CI/CD

- fix release pipeline ([2de1d55](https://github.com/ONREZA/prisma-adapter-bun/commit/2de1d55f87179050bae6f703b97d16f0aae22bd9)) by Ivan Bobchenkov

## [0.4.1] - 2026-02-05

### 🐛 Bug Fixes

- detect JSON strings in inferOidFromValue for relation joins ([4689143](https://github.com/ONREZA/prisma-adapter-bun/commit/4689143bad84939cf47734bb17a1ad066b50f347)) by Ivan Bobchenkov
- **ci:** restore registry-url for npm OIDC trusted publishing ([50d833f](https://github.com/ONREZA/prisma-adapter-bun/commit/50d833f200c912f8b12e034a23ae516023ce2c15)) by Ivan Bobchenkov

## [0.4.0] - 2026-02-05

### ✨ Features

- validate PostgreSQL connection URL parameters ([de93dae](https://github.com/ONREZA/prisma-adapter-bun/commit/de93daedc42321da62e4d874709b3dd0f49d4454)) by Ivan Bobchenkov

### 🐛 Bug Fixes

- **ci:** remove registry-url to fix OIDC trusted publishing ([ae47fad](https://github.com/ONREZA/prisma-adapter-bun/commit/ae47fad6d589d0a563ab5350f8189803c4ff1707)) by Ivan Bobchenkov
- **ci:** use npm publish with provenance for trusted publishing ([9dd77d3](https://github.com/ONREZA/prisma-adapter-bun/commit/9dd77d3b0190e36883f613d5a6bbdaba4b9ce97d)) by Ivan Bobchenkov
- **ci:** switch from npm publish to bun publish ([b461f69](https://github.com/ONREZA/prisma-adapter-bun/commit/b461f699640b0053e9688173fe5624440f26dec8)) by Ivan Bobchenkov

### 📝 Documentation

- update CLAUDE.md with project-specific instructions ([a65a9ca](https://github.com/ONREZA/prisma-adapter-bun/commit/a65a9ca4400a5a99e2f0b1ae6fd100d2e3ed5499)) by Ivan Bobchenkov
- add issue templates, PR template, CODEOWNERS, and community files ([2adbc06](https://github.com/ONREZA/prisma-adapter-bun/commit/2adbc06708a8b683fcc2c386fddd6a522d939736)) by Ivan Bobchenkov

## [0.3.2] - 2026-02-05

### 🐛 Bug Fixes

- **ci:** use skipHooks option to bypass lefthook during release ([a5ae981](https://github.com/ONREZA/prisma-adapter-bun/commit/a5ae9815b8f2f77d804f43ead2a4b06e2a375cb6)) by Ivan Bobchenkov
- **ci:** add biome format as onreza-release beforeCommit hook ([4995cd8](https://github.com/ONREZA/prisma-adapter-bun/commit/4995cd8fcafcd6715851cddc561d799215b1e766)) by Ivan Bobchenkov
- **ci:** merge release and publish into single workflow ([1a026e8](https://github.com/ONREZA/prisma-adapter-bun/commit/1a026e828d71a0a1f0f280d9876b2ab7de4028ba)) by Ivan Bobchenkov

### 🔧 Chores

- **deps:** update onreza-release to 2.4.1 ([44956d9](https://github.com/ONREZA/prisma-adapter-bun/commit/44956d99aaa738a12abd41f500a90fbad2f1a422)) by Ivan Bobchenkov

## [0.3.1] - 2026-02-05

### 🐛 Bug Fixes

- replace constructor parameter properties with explicit field declarations ([b0f7370](https://github.com/ONREZA/prisma-adapter-bun/commit/b0f737063bfeb63a6ce41be12a7571e7b090ae17)) by @Ivan Bobchenkov
- **ci:** use bunx for prepublishOnly, make prepare graceful ([2769b91](https://github.com/ONREZA/prisma-adapter-bun/commit/2769b914e6657f159b54fd43de6d0f16939b245a)) by @Ivan Bobchenkov

## [0.3.0] - 2026-02-05

### ✨ Features

- initial implementation of Prisma 7+ driver adapter for Bun.sql ([9c28243](https://github.com/ONREZA/prisma-adapter-bun/commit/9c2824348d156180f0e096ae146d761e805edb34)) by @Ivan Bobchenkov

### 🔧 Chores

- **ci:** add release scope to commitlint config ([c03ade8](https://github.com/ONREZA/prisma-adapter-bun/commit/c03ade87b442db3285c06e235400aaa05221c4f9)) by @Ivan Bobchenkov
- add biome, lefthook, commitlint and fix CI pipeline ([e592b43](https://github.com/ONREZA/prisma-adapter-bun/commit/e592b434aa3dc87d3d3b26263f70f65656c59389)) by @Ivan Bobchenkov

