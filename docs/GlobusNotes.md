# Installing and Configuring Globus Connect Personal on Linux:

https://docs.globus.org/how-to/globus-connect-personal-linux/
```bash
sudo yum install tk tcllib
mkdir -p ~/bin
cd ~/bin
mkdir -p globusconnectpersonal
wget https://downloads.globus.org/globus-connect-personal/linux/stable/globusconnectpersonal-latest.tgz
tar -xzf globusconnectpersonal-latest.tgz -C globusconnectpersonal --strip-components=1
cd globusconnectpersonal
./globusconnectpersonal
```
Since this is the first launch, and interactive setup GUI will appear and ask you to authenticate and such. Complete these steps.

Once these steps are complete, configure your acessible directories via the GUI by going to:
  `File > Preferences`
Note that the "Shared" column's checkboxes do not need to be checked, this refers to a sharing feature within Globus itself and in general you want them unchecked.

Click Save to confirm the changes.


## Headless Setup:

In general on my workstation I prefer to have Globus run silently in the background as a service. This allows me to access this endpoint whenever I need, including when I'm at home.

Create a new file `~/.config/systemd/user/globusconnectpersonal.service` and open it in your favorite text editor:
for example:
```bash
  nano ~/.config/systemd/user/globusconnectpersonal.service
```
Paste the following content into the file and save the changes (using Ctrl + o in nano)
```
[Unit]
Description=Globus Connect Personal
[Service]
ExecStart=%h/bin/globusconnectpersonal/globusconnectpersonal -start -debug
Restart=always
RestartSec=60
[Install]
WantedBy=default.target
```

Finally, setup your account for "lingering" and start the service with the following commands:
```bash

# If you want your user units to start on boot and persist after you logout, enable "lingering" for your user:
sudo loginctl enable-linger $USER

systemctl --user start globusconnectpersonal
systemctl --user enable globusconnectpersonal
systemctl --user status globusconnectpersonal

```

You should be ready to go! Open the following URL in the browser:
  https://app.globus.org/file-manager?origin_id=f418ea94-07aa-11ed-8d83-a54cf61939f8&origin_path=%2Fmedia%2FMAX%2F



-----


### Stopping the Globus Connect Personal Service:
systemctl --user stop globusconnectpersonal




# this is the contents of in.txt:
# a list of source paths followed by destination paths

file1.txt file1.txt
file2.txt file2.txt # inline-comments are also allowed
file3.txt file3.txt



https://globus-sdk-python.readthedocs.io/en/2.x-line/examples/advanced_transfer.html

Globus CLI Batch Transfer Recipe
https://github.com/globus/automation-examples/blob/master/batch.md


https://gcrnet.github.io/tutorials/globus_cli.html
https://docs.globus.org/cli/reference/task_event-list/

https://docs.globus.org/api/transfer/task/

https://github.com/CommanderPho/globus-automation-examples

https://docs.globus.org/cli/examples/


# Transfer Task
```json
{"DATA_TYPE":"task","bytes_checksummed":0,"bytes_transferred":27044729514076,"command":"API 0.10","deadline":"2023-03-12T15:22:22.000Z","delete_destination_extra":false,"destination_endpoint":"u_gjz3ny5efnehvgt65z3lxdqd74#b82d3b90-7b07-11ed-b303-55098fa75e99","destination_endpoint_display_name":"UMich ARC Non-Sensitive Data Den Volume Collection","destination_endpoint_id":"ab65757f-00f5-4e5b-aa21-133187732a01","directories":31722,"duration_at_last_fetch":1204297617,"effective_bytes_per_second":22456844,"encrypt_data":false,"fail_on_quota_errors":true,"faults":2083,"files":151957,"files_skipped":55510,"files_transferred":25295,"history_deleted":false,"is_delete":false,"is_paused":false,"is_transfer":true,"label":"Timer GDriveMount2DataDenData, run 1","nice_status":"UNKNOWN","nice_status_expires_in":-1,"nice_status_short_description":"unknown error","owner_id":"a3d80fc3-63c9-490c-9b9d-e5941faf1027","preserve_timestamp":false,"request_time":"2023-02-23T20:49:36.000Z","skip_source_errors":true,"source_endpoint":"u_upma7q3dzfeqzg454wkb7lyqe4#f418ea94-07aa-11ed-8d83-a54cf61939f8","source_endpoint_display_name":"Diba Lab Workstation 2022","source_endpoint_id":"f418ea94-07aa-11ed-8d83-a54cf61939f8","status":"ACTIVE","sync_level":0,"task_id":"92ae610e-b3bb-11ed-ae06-bfc1a406350a","type":"TRANSFER","username":"u_upma7q3dzfeqzg454wkb7lyqe4","verify_checksum":true}
```

ep1=ddb59aef-6d04-11e5-ba46-22000b92c6ec
ep2=ddb59af0-6d04-11e5-ba46-22000b92c6ec

# Bookmark IDs instead of endpoints
endpoint_gl_homedir=b0569b9c-2558-11ec-a0a7-6b21ca6daf73
endpoint_kdiba_lab_turbo=8ce139f8-9d46-11ed-a2a2-8383522b48d9

globus_endpoint_gl_homedir=e0370902-9f48-11e9-821b-02b7a92d8e58
globus_endpoint_kdiba_lab_turbo=8c185a84-5c61-4bbc-b12b-11430e20010f
globus_endpoint_pho_personal_laptop=20c84240-1eb1-11eb-81b7-0e2f230cc907



# List outputs
globus ls "${globus_endpoint_pho_personal_laptop}":/
globus ls "${globus_endpoint_pho_personal_laptop}":/Volumes/PegasusR6/Data/KDIBA


<!-- globus ls "${globus_endpoint_pho_personal_laptop_pegasusR6}":/ -->

# Transfering
% globus transfer --label "CLI Batch" --sync-level mtime --include "*.replay_info.mat" --source-endpoint "${endpoint_kdiba_lab_turbo}" --destination-endpoint "${endpoint_gl_homedir}" /Data/KDIBA/ /cloud/GDrive_Diba_Shared/Data/KDIBA/ --recursive 
globus transfer --label "CLI Batch" --sync-level mtime --include "*.replay_info.mat" "${endpoint_kdiba_lab_turbo}":/umms-dibalab/Data/KDIBA/ "${endpoint_gl_homedir}":/~/cloud/GDrive_Diba_Shared/Data/KDIBA/ --recursive 



## WORKING TRANSFER 2023-04-28:
globus transfer --label "CLI Batch Personal" --sync-level mtime --include "*.replay_info.mat" "${globus_endpoint_kdiba_lab_turbo}":/umms-kdiba/Data/KDIBA/ "${globus_endpoint_pho_personal_laptop}":/Volumes/PegasusR6/Data/KDIBA/ --recursive 


globus transfer --label "CLI Batch Personal" --sync-level mtime --include "*.replay_info.mat" "${globus_endpoint_kdiba_lab_turbo}":/umms-kdiba/Data/KDIBA/ "${globus_endpoint_pho_personal_laptop}":/Volumes/PegasusR6/Data/KDIBA/ --recursive 


"*.{xml,mat,pkl,npy,h5,eeg,dat}"





# Task Listing/Showing:
globus task show 92ae610e-b3bb-11ed-ae06-bfc1a406350a --successful-transfers --format json
globus task show 6db8af9a-bd7e-11ed-9c4d-2f882bacb908 --successful-transfers --format json
globus task show 6db8af9a-bd7e-11ed-9c4d-2f882bacb908 --skipped-errors --format json
```json
{
  "DATA": [
    {
      "DATA_TYPE": "skipped_error",
      "checksum_algorithm": null,
      "destination_path": "/umms-dibalab/Data/Hiro/",
      "error_code": "FILE_NOT_FOUND",
      "error_details": "550-GlobusError: v=1 c=PATH_NOT_FOUND%0D%0A550-GridFTP-Errno: 2%0D%0A550-GridFTP-Reason: System error in stat%0D%0A550-GridFTP-Error-String: No such file or directory%0D%0A550 End.%0D%0A",
      "error_time": "2023-03-08T06:57:05+00:00",
      "external_checksum": null,
      "is_delete_destination_extra": false,
      "is_directory": true,
      "is_symlink": false,
      "source_path": "/home/halechr/cloud/GDrive_Diba_Shared/Hiro/"
    },
    {
      "DATA_TYPE": "skipped_error",
      "checksum_algorithm": null,
      "destination_path": "/umms-dibalab/Data/Jahangir/",
      "error_code": "FILE_NOT_FOUND",
      "error_details": "550-GlobusError: v=1 c=PATH_NOT_FOUND%0D%0A550-GridFTP-Errno: 2%0D%0A550-GridFTP-Reason: System error in stat%0D%0A550-GridFTP-Error-String: No such file or directory%0D%0A550 End.%0D%0A",
      "error_time": "2023-03-08T06:57:05+00:00",
      "external_checksum": null,
      "is_delete_destination_extra": false,
      "is_directory": true,
      "is_symlink": false,
      "source_path": "/home/halechr/cloud/GDrive_Diba_Shared/Jahangir/"
    },
    {
      "DATA_TYPE": "skipped_error",
      "checksum_algorithm": null,
      "destination_path": "/umms-dibalab/Data/Laurel/",
      "error_code": "FILE_NOT_FOUND",
      "error_details": "550-GlobusError: v=1 c=PATH_NOT_FOUND%0D%0A550-GridFTP-Errno: 2%0D%0A550-GridFTP-Reason: System error in stat%0D%0A550-GridFTP-Error-String: No such file or directory%0D%0A550 End.%0D%0A",
      "error_time": "2023-03-08T06:57:05+00:00",
      "external_checksum": null,
      "is_delete_destination_extra": false,
      "is_directory": true,
      "is_symlink": false,
      "source_path": "/home/halechr/cloud/GDrive_Diba_Shared/Laurel/"
    }
  ]
}
```



"/home/halechr/cloud/GDrive_Diba_Shared/Hiro/"
"/umms-dibalab/Data/Laurel/"


# globus transfer --label "CLI Batch" --sync-level mtime --include "*.replay_info.mat" "${endpoint_kdiba_lab_turbo}":/umms-dibalab/Data/KDIBA/ "${endpoint_gl_homedir}":/~/cloud/GDrive_Diba_Shared/Data/KDIBA/ --recursive
