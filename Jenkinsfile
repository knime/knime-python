#!groovy
def BN = (BRANCH_NAME == 'master' || BRANCH_NAME.startsWith('releases/')) ? BRANCH_NAME : 'releases/2023-07'

@groovy.transform.Field
static final String[] WF_TESTS_PYTHON_ENVS = ['env_py36_pa5.yml', 'env_py38_pa7.yml', 'env_py39_kn47.yml']

@groovy.transform.Field
static final String DEFAULT_WF_TESTS_PYTHON_ENV = 'env_py39_kn47.yml'

library "knime-pipeline@$BN"

def baseBranch = (BN == KNIMEConstants.NEXT_RELEASE_BRANCH ? "master" : BN.replace("releases/",""))

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
    parameters(workflowTests.getConfigurationsAsParameters() + getWFTestsPythonEnvParameters()),
    buildDiscarder(logRotator(numToKeepStr: '5')),
    disableConcurrentBuilds()
])

try {
    knimetools.defaultTychoBuild('org.knime.update.python', 'maven && python-all && java17 && ubuntu22.04')

    node('ubuntu22.04 && python-all') {
        stage('Run pytest') {
            env.lastStage = env.STAGE_NAME
            checkout scm

            for (py in ['38', '39']) {
                sh """
                /home/jenkins/miniconda3/envs/knime_py${py}/bin/pytest --junit-xml=pytest_results_py${py}.xml --junit-prefix=py${py} || true
                """
                junit "pytest_results_py${py}.xml"
            }
        }
    }

    def parallelConfigs = [:]
    for (env in WF_TESTS_PYTHON_ENVS) {
        if (params[env]) {
            // need to create a deep copy here, otherwise Jenkins will use
            // the last selected option for everything
            def environmentFile = new String(env)
            parallelConfigs["${environmentFile}"] = {
                runPython3MultiversionWorkflowTestConfig(environmentFile, baseBranch)
            }
        }
    }

    parallel(parallelConfigs)

    stage('Sonarqube analysis') {
        env.lastStage = env.STAGE_NAME
        workflowTests.runSonar()
    }
 } catch (ex) {
     currentBuild.result = 'FAILURE'
     throw ex
 } finally {
     notifications.notifyBuild(currentBuild.result);
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
                        'knime-core-arrow',
                        'knime-testing-internal',
                        'knime-xml',
                        'knime-python-legacy',
                        'knime-conda',
                        'knime-python-nodes-testing',
                        'knime-base-views'
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
