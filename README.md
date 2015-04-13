# scrubulous

Analyze how your Ceph cluster performs (deep) scrubs

## Usage

    ./analyze-scrublogs.py

The input file should be generated using these commands:

    ceph osd tree
    ceph pg dump
    for osd_host in $osd_hosts
    do
      ssh $osd_host "zegrep 'scrub|slow request' /var/log/ceph/ceph-osd.*.{7,6,5,4,3,2,1}.gz"
    done

## Author

Simon Leinen, SWITCH  <simon.leinen@switch.ch>
