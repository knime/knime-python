export const executableOptionsMock = [
  {
    type: "PREF_BUNDLED",
    id: "",
    pythonExecutable: "/foo/bar/python",
    condaEnvName: null,
    condaEnvDir: null,
  },
  {
    type: "CONDA_ENV_VAR",
    id: "conda.environment1",
    pythonExecutable:
      "/opt/homebrew/Caskroom/miniconda/base/envs/conda_environment1/bin/python",
    condaEnvName: "conda_environment",
    condaEnvDir: "/opt/homebrew/Caskroom/miniconda/base/envs/conda_environment",
  },
  {
    type: "CONDA_ENV_VAR",
    id: "conda.environment2",
    pythonExecutable:
      "/opt/homebrew/Caskroom/miniconda/base/envs/conda_environment2/bin/python",
    condaEnvName: "conda_environment",
    condaEnvDir: "/opt/homebrew/Caskroom/miniconda/base/envs/conda_environment",
  },
];
