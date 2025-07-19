1. Construir a imagem Docker localmente (docker build)

    Cria a imagem Docker com o teu Dockerfile e o script entrypoint.sh

    Comando:

docker build -t gcr.io/SEU_PROJECT_ID/nome-da-imagem:tag .

    Explicação:
    docker build cria a imagem do container localmente usando o Dockerfile.
    O -t define a tag/nome da imagem.
    O prefixo gcr.io/SEU_PROJECT_ID diz que a imagem é para o Google Container Registry (GCR) do teu projeto.




2. Configurar Docker para autenticar com Google Container Registry

    Permite que o Docker faça push da imagem para o GCR autenticando com as credenciais da tua conta GCP

gcloud auth configure-docker




3. Fazer o push da imagem para o Google Container Registry (GCR)

    Comando:

docker push gcr.io/SEU_PROJECT_ID/nome-da-imagem:tag

    Explicação:
    Este comando envia (upload) a imagem que construíste para o repositório do Google Container Registry, que é um serviço da Google para armazenar imagens Docker de forma segura e acessível para os serviços GCP.




4. Criar e executar o Cloud Run Job com a imagem

    No Google Cloud Console, cria um Cloud Run Job usando a imagem que acabaste de subir.

    O Cloud Run Job vai executar o container que tu criaste, ou seja, vai fazer git clone/pull e rodar o dbt deps e dbt run.




5. No Airflow, criar um operador para chamar o Cloud Run Job

    Airflow vai ter uma task que invoca o Cloud Run Job através da API GCP.

    Pode usar o operador CloudRunJobExecuteOperator para disparar o job.

    Exemplo básico do operador:

from airflow.providers.google.cloud.operators.cloud_run import CloudRunJobExecuteOperator

run_dbt_job = CloudRunJobExecuteOperator(
    task_id="run_dbt_cloud_run_job",
    project_id="SEU_PROJECT_ID",
    location="us-central1",  # ou a região que usares
    job_name="nome-do-cloud-run-job",
    wait=True,  # espera o job acabar para continuar o DAG
)



Blocker:
->denied: Permission "artifactregistry.repositories.uploadArtifacts" denied on resource ...
->Conta não tem permissão para fazer upload de imagens para o Artifact Registry (ou GCR) - serviços para armazenar imagens docker na Google Cloud.