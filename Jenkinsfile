node {
    def app

    environment {
        DOCKERHUB_CREDENTIALS = credentials('docker-hub-credentials')
    }

    try {
        stage('Initialize'){
            def dockerHome = tool 'myDocker'
            env.PATH = "${dockerHome}/bin:${env.PATH}"
        }

        stage('Clone repository') {
            /* Let's make sure we have the repository cloned to our workspace */

            checkout scm
        }

        stage('Build image') {
            /* This builds the actual image; synonymous to
            * docker build on the command line */

            app = docker.build("sofraserv/edgarflaskdocker:${env.BUILD_NUMBER}")
        }

        stage('Login') {
            steps {
                sh 'echo $DOCKERHUB_CREDENTIALS_PSW | docker login -u $DOCKERHUB_CREDENTIALS_USR --password-stdin'
            }
        }

        stage('Push image') {
            /* Finally, we'll push the image with two tags:
            * First, the incremental build number from Jenkins
            * Second, the 'latest' tag.
            * Pushing multiple tags is cheap, as all the layers are reused.
            *docker.withRegistry('https://registry.hub.docker.com', 'docker-hub-credentials') {
            *    app.push("${env.BUILD_NUMBER}")
            *    app.push("latest")
            *}*/
            steps {
                sh 'docker push sofraserv/edgarflaskdocker:${env.BUILD_NUMBER}'
                sh 'docker push sofraserv/edgarflaskdocker:latest'
            }
        }
    } catch (e) {
		echo 'Error occurred during build process!'
		echo e.toString()
		currentBuild.result = 'FAILURE'
	} finally {
        junit '**/target/surefire-reports/TEST-*.xml'		
	}
}
