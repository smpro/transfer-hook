%{!?python_sitelib: %define python_sitelib %(%{__python} -c "from distutils.sysconfig import get_python_lib; print get_python_lib()")}

Name:           smhook
Version:        1
Release:        1%{?dist}
Summary:        Simple RPM used to setup the storage manager hook (python code)

Requires:       cms_sm_copyworker cms_sm_copymanager cms_sm_injectworker
Group:          CMS/System
License:        GPL
URL:            https://github.com/smpro/transfer-hook/tree/master/smhook
Source0:        %{name}.tgz
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

%description
This RPM installs the transfer system hook (python) and configuraton files. 

%prep
%setup -q -n smhook

%build

%install
rm -rf $RPM_BUILD_ROOT
mkdir -p $RPM_BUILD_ROOT/opt/python/
tar -xf /nfshome0/zdemirag/rpmbuild/SOURCES/smhook.tgz -C $RPM_BUILD_ROOT/opt/python/
mkdir -p $RPM_BUILD_ROOT/etc/init.d
install -m 755 smhookd $RPM_BUILD_ROOT/etc/init.d     
install -m 755 smeord $RPM_BUILD_ROOT/etc/init.d      

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root,-)
%doc
/opt/python/smhook/bookkeeper.py
/opt/python/smhook/daemon.py
/opt/python/smhook/enum.py
/opt/python/smhook/eor.py
/opt/python/smhook/__init__.py
/opt/python/smhook/macroeor.py
/opt/python/smhook/metafile.py
/opt/python/smhook/monitorRates.py
/opt/python/smhook/runinfo.py
/opt/python/smhook/smeord.py
/opt/python/smhook/smhookd.py
/opt/python/smhook/watchAndInject.py
/opt/python/smhook/config/__init__.py
/opt/python/smhook/config/runinfo/__init__.py
/opt/python/smhook/config/runinfo/driver_cfg.py
/opt/python/smhook/config/smeord.conf
/opt/python/smhook/config/smeord_test.conf
/opt/python/smhook/config/smhookd.conf
/opt/python/smhook/config/smhookd_test.conf
/etc/init.d/smeord
/etc/init.d/smhookd
/opt/python/smhook/config/__init__.pyc
/opt/python/smhook/config/runinfo/__init__.pyc
/opt/python/smhook/config/runinfo/driver_cfg.pyc
/opt/python/smhook/smeord
/opt/python/smhook/smhookd
/opt/python/smhook/smops/cleanup.py
/opt/python/smhook/smops/digest-copyworker-log.sh
/opt/python/smhook/smops/dump-eor-for-jira.sh
/opt/python/smhook/smops/fix_wbm.sh
/opt/python/smhook/smops/list-runs-and-times.sh
/opt/python/smhook/smops/list-runs.sh
/opt/python/smhook/smops/oracle.py
/opt/python/smhook/smops/testCompleteRun.py
/opt/python/smhook/testCompleteRun.py

%post
su - smpro -c "cat ~smpro/confidential/.db_int2r_cred.py" > /opt/python/smhook/config/.db_int2r_cred.py
chmod 400 /opt/python/smhook/config/.db_int2r_cred.py
su - smpro -c "cat ~smpro/confidential/.db.omds.runinfo_r.cfg.py" > /opt/python/smhook/config/.db.omds.runinfo_r.cfg.py
chmod 400 /opt/python/smhook/config/.db.omds.runinfo_r.cfg.py
su - smpro -c "cat ~smpro/confidential/.db_production_config.py" > /opt/python/smhook/config/.db_production_config.py
chmod 400 /opt/python/smhook/config/.db_production_config.py
su - smpro -c "cat ~smpro/confidential/.db_rcms_cred.py" > /opt/python/smhook/config/.db_rcms_cred.py
chmod 400 /opt/python/smhook/config/.db_rcms_cred.py
su - smpro -c "cat ~smpro/confidential/.db_runinfo_cred.py" > /opt/python/smhook/config/.db_runinfo_cred.py
chmod 400 /opt/python/smhook/config/.db_runinfo_cred.py
su - smpro -c "cat ~smpro/confidential/.smpro_cern_cred" > /opt/python/smhook/config/.smpro_cern_cred
chmod 400 /opt/python/smhook/config/.smpro_cern_cred
chkconfig --add smhookd
chkconfig --add smeord

%preun
if test -e /etc/init.d/smhookd; then
  /etc/init.d/smhookd stop
fi
if test -e /etc/init.d/smeord; then
  /etc/init.d/smeord stop
fi

%postun
chkconfig --del smhookd
chkconfig --del smeord
if [ "$1" = "0" ]; then
    rm -rf /opt/python/smhook/config/.db*
    rm -rf /opt/python/smhook/config/.sm*
    rm -rf /opt/python/smhook/
fi

%changelog
*Tue Apr 1 2015 <zeynep.demiragli@cern.ch> 2
--Updating initial build.
*Tue Mar 31 2015 <zeynep.demiragli@cern.ch> 1
--Initial Build.
