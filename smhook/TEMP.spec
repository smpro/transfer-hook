%{!?python_sitelib: %define python_sitelib %(%{__python} -c "from distutils.sysconfig import get_python_lib; print get_python_lib()")}

Name:           smhook
Version:        __VERSION__
Release:        __RELEASE__%{?dist}
Summary:        Simple RPM used to setup the storage manager hook (python code)

Requires:       cx_Oracle
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
mkdir -p $RPM_BUILD_ROOT/store/detectordata/
mkdir -p $RPM_BUILD_ROOT/store/detectordata/dpg_bril         
mkdir -p $RPM_BUILD_ROOT/store/detectordata/dpg_csc          
mkdir -p $RPM_BUILD_ROOT/store/detectordata/dpg_dt           
mkdir -p $RPM_BUILD_ROOT/store/detectordata/dpg_ecal         
mkdir -p $RPM_BUILD_ROOT/store/detectordata/dpg_hcal         
mkdir -p $RPM_BUILD_ROOT/store/detectordata/dpg_rpc          
mkdir -p $RPM_BUILD_ROOT/store/detectordata/dpg_tracker_pixel
mkdir -p $RPM_BUILD_ROOT/store/detectordata/dpg_tracker_strip
mkdir -p $RPM_BUILD_ROOT/store/detectordata/dpg_trigger      

tar -xf MYDIR/smhook.tgz -C $RPM_BUILD_ROOT/opt/python/
mkdir -p $RPM_BUILD_ROOT/etc/init.d
install -m 755 smhookd $RPM_BUILD_ROOT/etc/init.d     

%clean
rm -rf $RPM_BUILD_ROOT

%files -f ../../SPECS/files.list
%defattr(-,root,root,-)
%doc
/etc/init.d/smhookd
/opt/python/smhook/smhookd

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
chmod 444 /opt/python/smhook/config/.smpro_cern_cred
chkconfig --add smhookd
if test -e /etc/init.d/smhookd; then
  /etc/init.d/smhookd restart >/dev/null 2>&1
fi

%preun
if test -e /etc/init.d/smhookd; then
  /etc/init.d/smhookd stop >/dev/null 2>&1
fi

%postun
chkconfig --del smhookd
if [ "$1" = "0" ]; then
    rm -rf /opt/python/smhook/config/.sm*
    rm -rf /opt/python/smhook/
fi

%changelog
*Mon Jul 27 2015 <dylan.hsu@cern.ch> 3
--Made changes for private data
*Tue Apr 1 2015 <zeynep.demiragli@cern.ch> 2
--Updating initial build.
*Tue Mar 31 2015 <zeynep.demiragli@cern.ch> 1
--Initial Build.
