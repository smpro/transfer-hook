%{!?python_sitelib: %define python_sitelib %(%{__python} -c "from distutils.sysconfig import get_python_lib; print get_python_lib()")}

Name:           TEMP
Version:        1
Release:        1%{?dist}
Summary:        Simple RPM used to setup the storage manager hook (python code)

Requires:       cms_sm_copyworker cms_sm_copymanager cms_sm_injectworker
Group:          CMS/System
License:        GPL
URL:            https://github.com/smpro/transfer-hook/tree/master/TEMP
Source0:        %{name}.tgz
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

%description
This RPM installs the transfer system hook (python) and configuraton files. 

%prep
%setup -q -n TEMP

%build

%install
rm -rf $RPM_BUILD_ROOT
mkdir -p $RPM_BUILD_ROOT/opt/python/
tar -xf TEMP.tgz -C $RPM_BUILD_ROOT/opt/python/
mkdir -p $RPM_BUILD_ROOT/etc/init.d
install -m 755 TEMPd $RPM_BUILD_ROOT/etc/init.d     

%clean
rm -rf $RPM_BUILD_ROOT

%files -f files.list
%defattr(-,root,root,-)
%doc
/etc/init.d/TEMPd
/opt/python/smhook/TEMPd

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
chkconfig --add TEMPd

%preun
if test -e /etc/init.d/TEMPd; then
  /etc/init.d/TEMPd stop
fi

%postun
chkconfig --del TEMPd
if [ "$1" = "0" ]; then
    rm -rf /opt/python/TEMP/config/.db*
    rm -rf /opt/python/TEMP/config/.sm*
    rm -rf /opt/python/TEMP/
fi

%changelog
*Tue Apr 1 2015 <zeynep.demiragli@cern.ch> 2
--Updating initial build.
*Tue Mar 31 2015 <zeynep.demiragli@cern.ch> 1
--Initial Build.
