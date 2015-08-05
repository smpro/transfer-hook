%{!?python_sitelib: %define python_sitelib %(%{__python} -c "from distutils.sysconfig import get_python_lib; print get_python_lib()")}

Name:           smhookPDT
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
%setup -q -n smhookPDT

%build

%install
rm -rf $RPM_BUILD_ROOT
mkdir -p $RPM_BUILD_ROOT/opt/python/

tar -xf MYDIR/smhookPDT.tgz -C $RPM_BUILD_ROOT/opt/python/
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
mkdir -p /store/detectordata/dpg_bril         
mkdir -p /store/detectordata/dpg_csc          
mkdir -p /store/detectordata/dpg_dt           
mkdir -p /store/detectordata/dpg_ecal         
mkdir -p /store/detectordata/dpg_hcal         
mkdir -p /store/detectordata/dpg_rpc          
mkdir -p /store/detectordata/dpg_tracker_pixel
mkdir -p /store/detectordata/dpg_tracker_strip
mkdir -p /store/detectordata/dpg_trigger      
mkdir -p /store/detectordata/pdt_safety     
mkdir -p /store/detectordata/pdt_trash     
chown -R root:zh /store/detectordata/dpg_bril         
chown -R root:zh /store/detectordata/dpg_csc          
chown -R root:zh /store/detectordata/dpg_dt           
chown -R root:zh /store/detectordata/dpg_ecal         
chown -R root:zh /store/detectordata/dpg_hcal         
chown -R root:zh /store/detectordata/dpg_rpc          
chown -R root:zh /store/detectordata/dpg_tracker_pixel
chown -R root:zh /store/detectordata/dpg_tracker_strip
chown -R root:zh /store/detectordata/dpg_trigger      
chown -R root:zh /store/detectordata/pdt_safety     
chown -R root:zh /store/detectordata/pdt_trash     
chmod -R 775 /store/detectordata/dpg_bril         
chmod -R 775 /store/detectordata/dpg_csc          
chmod -R 775 /store/detectordata/dpg_dt           
chmod -R 775 /store/detectordata/dpg_ecal         
chmod -R 775 /store/detectordata/dpg_hcal         
chmod -R 775 /store/detectordata/dpg_rpc          
chmod -R 775 /store/detectordata/dpg_tracker_pixel
chmod -R 775 /store/detectordata/dpg_tracker_strip
chmod -R 775 /store/detectordata/dpg_trigger      
chmod -R 775 /store/detectordata/pdt_safety     
chmod -R 775 /store/detectordata/pdt_trash     

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
