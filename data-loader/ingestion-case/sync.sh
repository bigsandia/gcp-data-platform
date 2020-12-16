
CONFIG_BUCKET="another-data-platform-config"
RAW_BUCKET="another-data-platform-raw-1"


cases="delta overwrite"
for c in $cases
 do
   gsutil cp "$c/*.json" "gs://$CONFIG_BUCKET/"
   gsutil cp "$c/input/*" "gs://$RAW_BUCKET/$c/"

 done


