#!/usr/bin/python
import datetime

from ansible.module_utils.basic import AnsibleModule

ANSIBLE_METADATA = {
    'metadata_version': '1.0',
    'status': ['preview'],
    'supported_by': 'community'
}

DOCUMENTATION = '''
---
module: ceph_crush

author: Sebastien Han <seb@redhat.com>

short_description: Create Ceph CRUSH hierarchy

description:
    - By using the hostvar variable 'osd_crush_location'
    ceph_crush creates buckets and places them in the right CRUSH hierarchy
    - Only available in Ceph version luminous or greater.
'''

EXAMPLES = '''
- name: configure crush hierarchy
  ceph_crush:
    cluster: "{{ cluster }}"
    location: "{{ hostvars[item]['osd_crush_location'] }}"
  with_items: "{{ groups[osd_group_name] }}"
  when:
    - crush_rule_config
'''


'''
Declare CRUSH bucket types
'''
crush_bucket_types = [
    "host",
    "chassis",
    "rack",
    "row",
    "pdu",
    "pod",
    "room",
    "datacenter",
    "region",
    "root",
]


def get_ansible_var(module, variable):
    '''
    Extract the ansible variable from the playbook
    If variable doesn't exist, let's return None
    '''
    if variable in module.params["location"]:
        return module.params["location"][variable]


def generate_cmd(cluster, subcommand, bucket, bucket_type):
    '''
    Generate command line to execute
    '''
    return [
        'ceph',
        '--cluster',
        cluster,
        'osd',
        'crush',
        subcommand,
        bucket,
        bucket_type,
    ]


def create_and_move_buckets(module, cluster, osd_crush_location):
    '''
    Creates Ceph CRUSH buckets and arrange the hierarchy
    '''
    previous_bucket_type = None
    for k, crush_bucket_type in enumerate(crush_bucket_types):
        if crush_bucket_type in osd_crush_location.keys():
            cmd = generate_cmd(cluster, "add-bucket", osd_crush_location[crush_bucket_type], crush_bucket_type)
            rc, out, err = module.run_command(cmd, encoding=None)
            if previous_bucket_type:
                cmd = generate_cmd(cluster, "move", osd_crush_location[previous_bucket_type], crush_bucket_type + "=" + osd_crush_location[crush_bucket_type])
                rc, out, err = module.run_command(cmd, encoding=None)
            previous_bucket_type = crush_bucket_type
    return rc, cmd, out, err, previous_bucket_type


def run_module():
    module_args = dict(
        cluster=dict(type='str', required=False, default='ceph'),
        location=dict(type='dict', required=True),
    )

    module = AnsibleModule(
        argument_spec=module_args,
        supports_check_mode=True
    )

    # FIND A WAY TO EXPOSE DOCKER_EXEC_CMD
    '''
    docker_exec_cmd = get_ansible_var(module, "docker_exec_cmd")
    if docker_exec_cmd:
        cmd.insert(docker_exec_cmd)
        cmd.insert(" ")
    '''

    cluster = module.params['cluster']
    location = module.params['location']

    result = dict(
        changed=False,
        stdout='',
        stderr='',
        rc='',
        start='',
        end='',
        delta='',
    )

    if module.check_mode:
        return result

    if len(location) < 2:
        result["stdout"] = "You must specify at least 2 buckets."
        result['rc'] = 1
        module.exit_json(**result)

    if "host" not in location.keys():
        result["stdout"] = "You must specify a 'host' bucket."
        result['rc'] = 1
        module.exit_json(**result)

    # Case where 'host' is part of the dict but the rest is not valid
    previous_bucket_type = None
    for k, crush_bucket_type in enumerate(crush_bucket_types):
        if crush_bucket_type in location.keys() and crush_bucket_type != "host":
            previous_bucket_type = "Ok"
    if previous_bucket_type is None:
        result["stdout"] = "It seems a 'host' bucket was specified but the other bucket types are invalid"
        result['rc'] = 1
        module.exit_json(**result)

    startd = datetime.datetime.now()

    # run the Ceph command to add buckets
    rc, cmd, out, err, previous_bucket_type = create_and_move_buckets(module, cluster, location)

    endd = datetime.datetime.now()
    delta = endd - startd

    result = dict(
        cmd=cmd,
        start=str(startd),
        end=str(endd),
        delta=str(delta),
        rc=rc,
        stdout=out.rstrip(b"\r\n"),
        stderr=err.rstrip(b"\r\n"),
        changed=True,
    )

    if previous_bucket_type is None:
        result["stdout"] = "No valid CRUSH bucket type found, valid buckets can be found at http://docs.ceph.com/docs/master/rados/operations/crush-map/#crush-structure"
        result['rc'] = 1
        module.exit_json(**result)

    if rc != 0:
        module.fail_json(msg='non-zero return code', **result)

    module.exit_json(**result)


def main():
    run_module()


if __name__ == '__main__':
    main()
