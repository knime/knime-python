#!groovy
def BN = (BRANCH_NAME == 'master' || BRANCH_NAME.startsWith('releases/')) ? BRANCH_NAME : 'releases/2022-09'

@groovy.transform.Field
static final String[] PYTHON_VERSIONS = ['36', '37', '38', '39']

@groovy.transform.Field
static final String DEFAULT_PYTHON_VERSION = '39'

library "knime-pipeline@$BN"

def baseBranch = (BN == KNIMEConstants.NEXT_RELEASE_BRANCH ? "master" : BN.replace("releases/",""))

properties([
    pipelineTriggers([
        upstream('knime-filehandling/' + env.BRANCH_NAME.replaceAll('/', '%2F'))
    ]),
    parameters(workflowTests.getConfigurationsAsParameters() + getPythonParameters()),
    buildDiscarder(logRotator(numToKeepStr: '5')),
    disableConcurrentBuilds()
])

try {
    knimetools.defaultTychoBuild('org.knime.update.python', 'maven && python-all && java11')

    node('ubuntu20.04 && python-all') {
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
    for (py in PYTHON_VERSIONS) {
        if (params[py]) {
            // need to create a deep copy here, otherwise Jenkins will use
            // the last selected option for everything
            def python_version = new String(py)
            parallelConfigs["${python_version}"] = {
                runPython3MultiversionWorkflowTestConfig(python_version, baseBranch)
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

/**
* Return parameters to select python version to run workflowtests with
*/
def getPythonParameters() {
    def pythonParams = []
    for (c in PYTHON_VERSIONS) {
        pythonParams += booleanParam(defaultValue: c == DEFAULT_PYTHON_VERSION, description: "Run workflowtests with Python ${c}", name: c)
    }
    return pythonParams
}

def runPython3MultiversionWorkflowTestConfig(String pythonVersion, String baseBranch) {
    withEnv([ "KNIME_WORKFLOWTEST_PYTHON_VERSION=${pythonVersion}" ]) {
        stage("Workflowtests with Python: ${pythonVersion}") {
            workflowTests.runTests(
                testflowsDir: "Testflows (${baseBranch})/knime-python/python3.multiversion",
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
                extraNodeLabel: 'python-all'
            )
        }
    }
}

/* vim: set shiftwidth=4 expandtab smarttab: */
