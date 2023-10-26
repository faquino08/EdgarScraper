node {
    def app

    environment {
        DOCKERHUB_CREDENTIALS = credentials('docker-hub-credentials')
    }

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
        sh 'docker login -u ${DOCKERHUB_CREDENTIALS_USR} -p ${DOCKERHUB_CREDENTIALS_PSW} https://registry.hub.docker.com'
    }

    stage('Push image') {
        sh 'docker push sofraserv/edgarflaskdocker:${env.BUILD_NUMBER}'
        sh 'docker push sofraserv/edgarflaskdocker:latest'
    }
}
