#!groovy
def BN = (BRANCH_NAME == 'master' || BRANCH_NAME.startsWith('releases/')) ? BRANCH_NAME : 'releases/2022-06'

@groovy.transform.Field
static final String[] PYTHON_VERSIONS = ['36', '37', '38', '39']

@groovy.transform.Field
static final String DEFAULT_PYTHON_VERSION = '36'

library "knime-pipeline@$BN"

def baseBranch = (BN == KNIMEConstants.NEXT_RELEASE_BRANCH ? "master" : BN)

properties([
    pipelineTriggers([
        upstream('knime-filehandling/' + env.BRANCH_NAME.replaceAll('/', '%2F'))
    ]),
    parameters(workflowTests.getConfigurationsAsParameters() + getPythonParameters()),
    buildDiscarder(logRotator(numToKeepStr: '5')),
    disableConcurrentBuilds()
])

try {
    knimetools.defaultTychoBuild('org.knime.update.python', 'maven && python2 && python3 && java11')

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

    // legacy tests
    parallelConfigs["Python 2.7"] = {
        runPython27WorkflowTests(baseBranch)
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

def runPython27WorkflowTests(String baseBranch){
    withEnv([ "KNIME_POSTGRES_USER=knime01", "KNIME_POSTGRES_PASSWORD=password",
              "KNIME_MYSQL_USER=root", "KNIME_MYSQL_PASSWORD=password",
              "KNIME_MSSQLSERVER_USER=sa", "KNIME_MSSQLSERVER_PASSWORD=@SaPassword123",]){

        workflowTests.runTests(
            testflowsDir: "Testflows (${baseBranch})/knime-python/python2.7",
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
                    'knime-xml'
                ],
                ius: [ 'org.knime.features.ext.jython.feature.group', 'org.knime.features.chem.types.feature.group' ]
            ],
            extraNodeLabel: 'python2',
            sidecarContainers: [
                    [ image: "${dockerTools.ECR}/knime/postgres:12", namePrefix: "POSTGRES", port: 5432,
                        envArgs: [
                            "POSTGRES_USER=${env.KNIME_POSTGRES_USER}", "POSTGRES_PASSWORD=${env.KNIME_POSTGRES_PASSWORD}",
                            "POSTGRES_DB=knime_testing"
                        ]
                    ],
                    [ image: "${dockerTools.ECR}/knime/mysql5", namePrefix: "MYSQL", port: 3306,
                        envArgs: ["MYSQL_ROOT_PASSWORD=${env.KNIME_MYSQL_PASSWORD}"]
                    ],
                    [ image: "${dockerTools.ECR}/knime/mssql-server", namePrefix: "MSSQLSERVER", port: 1433,
                        envArgs: ["ACCEPT_EULA=Y", "SA_PASSWORD=${env.KNIME_MSSQLSERVER_PASSWORD}", "MSSQL_DB=knime_testing"]
                    ]
            ],
        )
    }
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
                        'knime-xml'
                    ],
                    ius: [ 'org.knime.features.chem.types.feature.group' ]
                ],
                extraNodeLabel: 'python-all'
            )
        }
    }
}

/* vim: set shiftwidth=4 expandtab smarttab: */
