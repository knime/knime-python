#!groovy
def BN = (BRANCH_NAME == 'master' || BRANCH_NAME.startsWith('releases/')) ? BRANCH_NAME : 'releases/2024-06'

@groovy.transform.Field
static final String[] WF_TESTS_PYTHON_ENVS = [
    'bundled',
    'env_py36_pa5.yml',
    'env_py38_pa7.yml',
    'env_py39_kn47.yml',
]

@groovy.transform.Field
static final String DEFAULT_WF_TESTS_PYTHON_ENV = 'bundled'

@groovy.transform.Field
static final List<String> PYTEST_PYTHON_ENVS = ['env_py38_legacy', 'env_py38', 'env_py39', 'env_py311']

library "knime-pipeline@$BN"

def baseBranch = (BN == KNIMEConstants.NEXT_RELEASE_BRANCH ? 'master' : BN.replace('releases/', ''))

/** Return parameters to select python environment to run workflowtests with */
def getWFTestsPythonEnvParameters() {
    def pythonParams = []
    for (c in WF_TESTS_PYTHON_ENVS) {
        pythonParams += booleanParam(
            defaultValue: c == DEFAULT_WF_TESTS_PYTHON_ENV || BRANCH_NAME.startsWith('releases/'),
            description: "Run workflowtests with Python environment ${c}",
            name: c
        )
    }
    return pythonParams
}

properties([
    pipelineTriggers([
        upstream("knime-conda-channels/${BRANCH_NAME.replaceAll('/', '%2F')}, " +
            "knime-python-nodes-testing/${BRANCH_NAME.replaceAll('/', '%2F')}, " +
            "knime-core-ui/${BRANCH_NAME.replaceAll('/', '%2F')}")
    ]),
    parameters(
        workflowTests.getConfigurationsAsParameters() + \
        getWFTestsPythonEnvParameters() + \
        booleanParam(defaultValue: false, description: 'Upload conda package despite tests being unstable or being on master.', name: 'UPLOAD_ANYWAY')
    ),
    buildDiscarder(logRotator(numToKeepStr: '5')),
    disableConcurrentBuilds()
])

try {
    knimetools.defaultTychoBuild('org.knime.update.python', 'maven && java17 && ubuntu22.04 && workflow-tests')

    node('ubuntu22.04 && workflow-tests && java17') {
        stage('Prepare for pytest') {
            env.lastStage = env.STAGE_NAME
            checkout scm
        }

        for (pyEnv in PYTEST_PYTHON_ENVS) {
            stage("Run pytest for ${pyEnv}") {
                env.lastStage = env.STAGE_NAME

                String envPath = "${env.WORKSPACE}/pytest-envs/${pyEnv}"
                String envYml = "${env.WORKSPACE}/pytest-envs/${pyEnv}.yml"

                sh(label: 'create conda env', script: """
                    micromamba create -p ${envPath} -f ${envYml}
                """)

                sh(label: 'run pytest', script: """
                    ${envPath}/bin/coverage run -m pytest --junit-xml=pytest_results.xml || true

                    # create a separate coverage.xml file for each module
                    for d in org.knime.python3*/ ; do
                        ${envPath}/bin/coverage xml -o "\${d}coverage-${pyEnv}.xml" --include "*\$d**/*.py" || true

                        # delete mention of module name in coverage.xml
                        if [ -f "\${d}coverage-${pyEnv}.xml" ]; then
                            sed -i "s|\$d||g" "\${d}coverage-${pyEnv}.xml"
                        fi
                    done
                """)

                junit 'pytest_results.xml'
                stash(name: "${pyEnv}", includes: "**/coverage-${pyEnv}.xml")
            }
        }
    }

    def parallelConfigs = [:]
    for (env in WF_TESTS_PYTHON_ENVS) {
        if (params[env]) {
            // need to create a deep copy here, otherwise Jenkins will use
            // the last selected option for everything
            String environmentFile = new String(env)
            parallelConfigs["${environmentFile}"] = {
                runPython3MultiversionWorkflowTestConfig(environmentFile, baseBranch)
            }
        }
    }

    parallel(parallelConfigs)

    // Only build if on master and tests pass
    if (params.UPLOAD_ANYWAY || BN == 'master' && currentBuild.result == 'STABLE') {
        node('ubuntu22.04') {

            stage('Build and Deploy knime-extension ') {

                env.lastStage = env.STAGE_NAME
                checkout scm

                String envName = "test_knime_extension"
                String recipePath = "${env.WORKSPACE}/knime-extension/recipe"
                String prefixPath = "${env.WORKSPACE}/${envName}"
                String[] packageNames = [
                    "conda-build",
                    "anaconda-client"
                ]

                condaHelpers.createCondaEnv(prefixPath: prefixPath, packageNames: packageNames)

                sh(
                    label: "Collect Files",
                    script: """#!/bin/sh
                        cd knime-extension
                        micromamba run -p ${prefixPath} python ${env.WORKSPACE}/knime-extension/collect_files.py
                    """
                )

                condaHelpers.buildCondaPackage(recipePath, prefixPath, true)

            }
        }
    }


    stage('Sonarqube analysis') {
        env.lastStage = env.STAGE_NAME
        env.SONAR_ENV = "Sonarcloud"
        configs = workflowTests.ALL_CONFIGURATIONS + PYTEST_PYTHON_ENVS
        echo "running sonar on ${configs}"
        workflowTests.runSonar(configs)
    }
 } catch (ex) {
    currentBuild.result = 'FAILURE'
    throw ex
 } finally {
    notifications.notifyBuild(currentBuild.result)
}

def runPython3MultiversionWorkflowTestConfig(String environmentFile, String baseBranch) {
    withEnv([ "KNIME_WORKFLOWTEST_PYTHON_ENVIRONMENT=${environmentFile}" ]) {
        stage("Workflowtests with Python: ${environmentFile}") {
            workflowTests.runTests(
                dependencies: [
                    repositories: [
                        'knime-chemistry',
                        'knime-database',
                        'knime-office365',
                        'knime-datageneration',
                        'knime-distance',
                        'knime-ensembles',
                        'knime-filehandling',
                        'knime-jep',
                        'knime-jfreechart',
                        'knime-js-base',
                        'knime-kerberos',
                        'knime-python',
                        'knime-core-columnar',
                        'knime-testing-internal',
                        'knime-xml',
                        'knime-python-legacy',
                        'knime-conda',
                        'knime-python-nodes-testing',
                        'knime-base-views',
                        'knime-scripting-editor',
                        'knime-gateway',
                        'knime-credentials-base',
                        'knime-google',
                        'knime-cloud',
                        'knime-buildworkflows'
                    ],
                    ius: [
                        'org.knime.features.chem.types.feature.group',
                        'org.knime.features.core.columnar.feature.group'
                    ]
                ],
            )
        }
    }
}

/* vim: set shiftwidth=4 expandtab smarttab: */
