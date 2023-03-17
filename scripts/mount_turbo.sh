MOUNT_NAME=umms-kdiba
STORAGE_VOLUME="${MOUNT_NAME}.turbo.storage.umich.edu:/${MOUNT_NAME}"
MOUNT_LOCATION="${HOME}/cloud/turbo_umms_kdiba"
mkdir -p "${MOUNT_LOCATION}"
sudo mount -t nfs "${STORAGE_VOLUME}" "${MOUNT_LOCATION}"

