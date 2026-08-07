pipeline {
  options {
    disableConcurrentBuilds()
  }

  agent any

  environment {
    dockerImageName = "rubincr.lsst.org/nightlydigest-backend:"
    dockerImage = ""
  }

  stages {
    stage("Setup and run pre-commit") {
      agent {
        docker {
          alwaysPull true
          image 'lsstts/develop-env:develop'
          args "--entrypoint=''"
        }
      }
      steps {
        script {
          sh """
            source /home/saluser/.setup_dev.sh || echo loading env failed. Continuing...
            generate_pre_commit_conf --skip-pre-commit-install
            pre-commit run --all
          """
        }
      }
    }

    stage("Run tests") {
      agent {
        docker {
          alwaysPull true
          image 'lsstts/develop-env:develop'
          args "--entrypoint=''"
        }
      }
      steps {
        script {
          sh """
            source /home/saluser/.setup_dev.sh || echo loading env failed. Continuing...
            pip install -e .
            pytest -v
          """
        }
      }
    }

    stage("Build Docker image") {
      when {
        anyOf {
          branch "develop"
        }
      }
      steps {
        script {
          image_tag = "develop"
          dockerImageName = dockerImageName + image_tag
          echo "dockerImageName: ${dockerImageName}"
          dockerImage = docker.build(dockerImageName, "-f docker/Dockerfile-deploy .")
        }
      }
    }
    
    stage("Push Docker image") {
      when {
        anyOf {
          branch "develop"
        }
      }
      steps {
        script {
          docker.withRegistry("https://rubincr.lsst.org/", "nexus3-lsst_jenkins") {
            dockerImage.push()
          }
        }
      }
    }
  }
}
