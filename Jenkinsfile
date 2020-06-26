#!groovy
def BN = BRANCH_NAME == "master" || BRANCH_NAME.startsWith("releases/") ? BRANCH_NAME : "master"

library "knime-pipeline@$BN"

properties([
    // provide a list of upstream jobs which should trigger a rebuild of this job
    pipelineTriggers([
        upstream('knime-filehandling/' + env.BRANCH_NAME.replaceAll('/', '%2F'))
	]),
    buildDiscarder(logRotator(numToKeepStr: '5')),
    disableConcurrentBuilds()
])

try {
	// provide the name of the update site project
	knimetools.defaultTychoBuild('org.knime.update.python', 'maven && python2 && python3')

	workflowTests.runTests(
        dependencies: [
            repositories: [
                'knime-chemistry',
                'knime-database',
                'knime-datageneration',
                'knime-distance',
                'knime-ensembles',
                'knime-filehandling',
                'knime-jep',
                'knime-jfreechart',
                'knime-js-base',
                'knime-kerberos',
                'knime-python',
                'knime-testing-internal',
                'knime-xml'
            ]
        ]
	)

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
/* vim: set shiftwidth=4 expandtab smarttab: */
