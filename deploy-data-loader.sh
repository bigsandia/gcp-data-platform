
_ARTIFACT_NAME="data-loader"
_PROJECT="another-data-platform"
SHORT_SHA=$(git rev-parse --short HEAD)

gcloud run deploy data-loader \
        --image eu.gcr.io/"${_PROJECT}"/run/"${_ARTIFACT_NAME}":"$SHORT_SHA" \
        --region=europe-west1 \
        --concurrency=1 \
        --max-instances=1 \
        --platform=managed \
        --no-allow-unauthenticated \
        --set-env-vars DATA_LOCATION=US \
        --service-account=data-loader-invoker@$_PROJECT.iam.gserviceaccount.com