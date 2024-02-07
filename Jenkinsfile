pipeline {
  agent any
  environment {
    COMMIT = sh(script: 'git rev-parse --short HEAD', returnStdout: true).trim()
  }
  stages {
    stage('Git pull') {
      steps {
        sh 'ssh katie-002 \'cd /home/omgili/files/DuplicateService && git checkout master && git pull\''
      }
    }
    stage('Run Playbook') {
      steps {
        sh 'ssh katie-002 \'source /home/omgili/.ansible/venv/bin/activate && cd /home/omgili/ansible/ && ansible-playbook playbooks/duplicate_service.yml --extra-vars=\'commit=$COMMIT\' -v\''
      }
    }
  }
  post {  
    failure {  
      sh "echo Sending Failed Email!"
        emailext body: 'CI/CD Pipeline has been failed!',
        subject: 'CI/CD pipeline failed!',
        mimeType: 'text/html',to: 'devops-alerts-aaaaimnya6pdacs5sfd7ewl7me@webz-io.slack.com'
    } 
  }
}

