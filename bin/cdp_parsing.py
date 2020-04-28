#!/usr/bin/env python
import numpy as np
import pandas as pd
import argparse
parser=argparse.ArgumentParser()
parser.add_argument("-i","--input_csv",help='Input raw csv')
parser.add_argument("-o","--output_csv",help='Output parsed csv')
args = parser.parse_args() 
"""
numfields=['ADMISS','ADMISSRS','ADMISSTO','ADULTATND','ADVERTCD','ADVERTRS','ADVERTTO','ADVOCENG','ADVPRDEV','ADVPRGA','ADVPRPRG','ADVPRSF','ADVPRTOT','AFTA','ALLEDFR','ALLEDPD','APPFEECD','ARAG','ARTSALCD','AUDIT','BDCASH','BDCORPUS','BDENDDRW','BDENDINV','BDMAXDRW','BDOTH','BENSF','BENTOT','BOARDCD','BOARDHRS','BONDPYCUR','BONDPYLT','CAPEXPTOT','CAPTO','CAPUN','CASHAG','CASHCD','CASRESRVTOT','CDPTAXON','CITYCD','CITYNARCD','CITYNCD','CITYSF','CITYTO','CLIAOTAG','CLMGCNDEV','CLMGCNGA','CLMGCNPRG','CLMGCNTOT','CNCESSCD','CNCESSRS','CNCESSTO','CNFHSPDEV','CNFHSPGA','CNFHSPPRG','CNFHSPSF','CNFHSPTOT','CNTART','CNTRS','CNTOTPMCD','CNTOTTMCD','CNTOTTOCD','CNTOTUNCD','CNTYNARCD','COMCNSGN','CONSTRUCT','CONSULTHIFEE','CONSULTLOFEE','CONSULTN','CONSVC','CONSVCRS','CONSVCTO','CORPCD','CORPNCD','CORPPM','CORPSF','CORPTM','CORPTO','COUNTYCD','COUNTYNCD','COUNTYPM','COUNTYSF','COUNTYTM','COUNTYTO','CRLNEOY','CRLNLIM','CRPNARCD','CURASSAG','CURLIAAG','CYEAR','CYRFOUND','CZIP','CZIP4','DEFREVAG','DEPRCNCD','DEVBENCD','DEVCONTO','DEVSALCD','DIGADS','DIGSHOW','DMAILN','DPRCNDEV','DPRCNGA','DPRCNPRG','DPRCNSF','DPRCNTO','DUESDEV','DUESGA','DUESPRG','DUESSF','DUESTOT','DUNS','ED3INCCDN','ED3INCRS','ED3INCTON','EDPERFS','EDPUBPROF','EDSERIES','EDSERPERFS','EIN','EMAILN','EQUIPCD','ERNNCAPCD','EXHEXPDEV','EXHEXPGA','EXHEXPPRG','EXHEXPTOT','EXHWKSTOT','FBFLWRS','FEDCD','FEDNARCD','FEDNCD','FEDPM','FEDSF','FEDTM','FEDTO','FESTATTOT','FESTSHOW','FISCSPEIN','FILMS','FILMSCRN','FIXDASSAG','FIXDASSCD','FIXDASSPM','FIXDASSTM','FIXDASSUN','FLKFLWRS','FNDNARCD','FOUNDCD','FOUNDNCD','FOUNDPM','FOUNDSF','FOUNDTM','FOUNDTO','FRATNDCD','FRATNDSF','FSAFCD','FSAFRS','FSAFTO','FTACARCD','FTARTPER','FTEMPS','FTSEAS','FTSEASFTE','FTVOLS','FYEND','GA1099','GABENCD','GALSALTM','GALSALTO','GALSALUN','GASAL','GDTOURRS','GDTOURTO','GDTOURUN','GFTSLS','GFTSLSRS','GFTSLSTO','GOVCD','GOVN','GOVPM','GOVTM','GOVTO','GPFLWRS','GRANTDEV','GRANTGA','GRANTPRG','GRANTSF','GRANTTOT','GRPINRS','GRPINTO','GRPINUN','GUIDTPERF','GUIDTSHOW','HASADVO','HASAFTA','HASAHC','HASARTSED','HASAUDIT','HASBCAST','HASBHS','HASCOMM','HASCOMP','HASCONF','HASDEVS','HASENDW','HASEXHS','HASFESTS','HASFS','HASGRNTS','HASLAO','HASLF','HASMEDSUB','HASMEMBR','HASNGLD','HASNONOPE','HASNONOPR','HASOPAM','HASOPREH','HASPERFS','HASPUB','HASRESIDENC','HASRESRCH','HASRSREV','HASSCREENS','HASSF','HASSUBEVS','HASTOURS','HISUBCD','HITIXMUS','HITIXPERF','HITXIMEMB','HITXOMEMB','ICART','ICTOT','IGFLWRS','IKB','IKCAO','IKCE','IKCIP','IKL','IKLBI','IKO','INDMEMLPSN','NewMembs','INKIND','INKINDOPPM','INKINDOPTM','INKINDOPTO','INKINDOPUN','INKINDPM','INKINDSF','INKINDTM','INKINDTO','INKNDNOPPM','INKNDNOPTM','INKNDNOPTO','INKNDNOPUN','INSURCD','INSURDEV','INSURGA','INSURPRG','INSURSF','INTEXDEV','INTEXGA','INTEXPRG','INTEXSF','INTEXTOT','INTFXDASS','INTNTOT','INVAG','INVRESRVTOT','IRSDATE','KIDATTND','KIDATTPSF','KIDSINSCHLS','LECOCC','LOANINCD','LOANINRS','LOANINTO','LOANSCURAG','LOANSCURPM','LOANSCURTM','LOANSCURTO','LOANSCURUN','LOANSLTAG','LOANSLTPM','LOANSLTTM','LOANSLTTO','LOANSLTUR','LOCART','LOCARTSF','LOCARTTOT','LOCPREM','LOCPREMEXH','LOCPREMPRF','LOSUBCD','LOTIXIMEMB','LOTIXMUS','LOTIXOMEMB','LOTIXPERF','LTASSAG','LTASSSF','LTINVAG','LTLIAAG','LTLIACD','LTLIAPM','LTLIATM','LTLIAUN','MBRINC','MBRINCTO','MBRINDFREN','MBRINDTOTN','MBRINRS','MBRORGFRN','MBRORGPDN','MBRORGTOTN','MBROTHFRN','MBROTHPDN','MBROTHTOTN','MEMBERS','MEMRETRNN','MKTPFO','MKTSAT','MKTTOT','MKTTOTSF','MOCOUNT','MORTCUR','MORTLT','NARTRCD','NARTRCDX','NARTRPM','NARTRTM','NARTRTO','NATPREM','NATPRMEXH','NATPRMPRF','NETINCCD','NETINCNOP','NETINCOP','NETINCSF','NETINCUN','NewSubs','NISPID','NLOCART','NLOCARTSF','NLOCARTTOT','NOAUDITAG','NOCOMMISS','NOLECTUR','NONOPEXPTOT','NONOPSURP','NOOTPRG','NOTOPPM','NOTOPTM','NOTOPTO','NOTOPUN','NOTOPXFER','NOTOPXFERPM','NOTOPXFERTM','NOTOPXFERTO','NPERSEXDEV','NPERSEXGA','NPERSEXPRG','NPERSEXTOT','NVSTINOPCD','NVSTINOPPM','NVSTINOPTM','NVSTINOPTO','NVSTINOTCD','NVSTINOTPM','NVSTINOTTM','NVSTINOTTO','OACTSAT','OCCDEV','OCCGA','OCCPROG','OCCSF','OCCTOT','OCNTART','OCOMCNSGN','OFFCDEV','OFFCGA','OFFCPRG','OFFCTOT','OFFEDSHW','OFFSF','OFXDASS','ONONOPEXDEV','ONONOPEXGA','ONONOPEXPRG','OOPEXDEV','OOPEXGA','OOPEXPRG','OOPEXSF','OOPEXTOT','OPEARNPM','OPEARNTM','OPEXPTOT','OPPM','OPREHTOT','OPRGRVCD','OPRGRVTM','OPRGRVTO','OPSF','OPSURPCDD','OPTM','OPTO','OPUN','ORGID','ORGLAT','ORGLONG','ORGMEMLPSN','ORGMEMNEWN','ORGMEMRETRN','OrgYrFound','OSMFLWRS','OTARTTOT','OTCURASAG','OTDON','OTDONPM','OTDONTM','OTDONTO','OTHEARNCD','OTHEARNRS','OTHEARNSF','OTHEARNTO','OTHERMKTG','OTHINDCD','OTHINDPM','OTHINDSF','OTHINDTM','OTHINDTO','OTHLNCUR','OTHLNLT','OTHMEMLPSN','OTHMEMNEWN','OTMEMRETRNN','OTHNARCD','OTHRENTRS','OTHRENTTO','OTHRENTUN','OTINDNCD','OTLTASSAG','OTPMCUR','OTPRGPRF','OTRESRVTOT','OTTMCUR','OTTOCUR','OTUNCUR','PACTSAT','PARK','PARKRS','PARKTO','PAYABAG','PAYABPM','PAYABTM','PAYABTO','PAYABUN','PCNTART','PCOMCNSGN','PDACARCD','PDATNDADM','PDATNDPERF','PEIN','PERSNEXDEV','PERSNEXGA','PERSNEXPRG','PERSNEXSF','PFEEFUND','PFEEGA','PFEEPRG','PFEESF','PFEETOT','PHATTTOT','PINFLWRS','PLGRAG','PLGRCURTOT','PLGRTLTAG','PLGRTLTTO','PMACEXP','PMARECVCD','PMCASH','PMCASHTO','PMCLIAOT','PMCORPUS','PMCURASS','PMCURINV','PMCURLIA','PMDEFREV','PMENDDRW','PMEXSHOW','PMIFBALA','PMIFBALL','PMINVTO','PMLTASS','PMLTINV','PMLTLIAOT','PMMAXDRW','PMOTHASS','PMOTHTOT','PMPLGRCUR','PMPLGRTLT','PMPMCASH','PMPMEND','PMPMINV','PMPMOTH','PMPPE','PMTOTASS','PNOEXDEV','PNOEXGA','PNOEXPRG','PNOEXTOT','PPADJPM','PPADJTM','PPADJTO','PPADJUN','PREPADAG','PREPADCD','PRFARTTOT','PRGBENCD','PRODEXDEV','PRODEXGA','PRODEXPRG','PRODEXTOT','PRRADTVADS','PRSFEERS','PRSFEVTTO','PRTSHDEV','PRTSHGA','PRTSHPRG','PRTSHSF','PRTSHTOT','PRVEXPERTS','PRVSPACE','PTARTPER','PTEMPS','PTFTES','PTSEAS','PTSEASFTE','PTVFTES','PTVOLS','PUBDIST','PUBPRINT','PUBSALTM','PUBSALTO','PUBSALUN','PVTLESSONS','PVTSTUDS','RECDEV','RECGA','RECPRG','RECTOT','RESRPTS','RESRVDRW','RESRVMAXDR','RESRVTOT','ROYALCD','ROYALRS','ROYALTO','RYLDEV','RYLEXPCD','RYLGA','RYLTOT','SALSF','SALTO','SENATTND','SHLTRCD','SHLTREIN','SMORG','SMEDFLWRS','SNONOPEXDV','SNONOPEXGA','SNONOPEXPR','SOPEXDEV','SOPEXGA','SOPEXPRG','SOPEXSF','SPC1SQFT','SPC1ZIP','SPC2SQFT','SPC2ZIP','SPC3SQFT','SPC3ZIP','SPC4SQFT','SPC4ZIP','SPC5SQFT','SPC5ZIP','SPCRENTRS','SPCRENTTO','SPCRENTUN','SPEVCRPN','SPEVGRPM','SPEVGRTM','SPEVGRTO','SPEVGRUN','SPEVINDN','SPEVNETPM','SPEVNETTM','SPEVNETTO','SPEVNETUN','SPEVOTHN','SPEVTOTN','SPNSOR','SPNSORRS','SPNSORTO','SPRENTSQFT','STANARCD','STATECD','STATENCD','STATEPM','STATESF','STATETM','STATETO','STINCRS','STINCTO','STXINCUN','SUBATTF','SUBATTP','SUBBCSTCD','SUBBCSTRS','SUBBCSTTO','SUBINCFLRS','SUBINCFLTO','SUBINCFLX','SUBINCFUL','SUBINCFXRS','SUBINCFXTO','SUBMEDBFR','SUBMEDBPD','SUBMEDBTO','SUBMEDPFR','SUBMEDPPD','SUBMEDPTO','SUBPUBCD','SUBPUBRS','SUBPUBTO','SUBRETRNN','SUBSTATTOT','SUGDONAMT','TCNTCDOPSF','TKTADMFR','TKTADMPD','TKTADMTO','TMACEXP','TMARECVCD','TMCARESRV','TMCASH','TMCASHTO','TMCLIAOT','TMCORPUS','TMCURASS','TMCURINV','TMCURLIA','TMDEFREV','TMENDBSCHK','TMENDDRW','TMEXSHOW','TMFLWRS','TMIFBALA','TMIFBALL','TMINVRESRV','TMINVTO','TMLTASS','TMLTINV','TMLTLIAOT','TMMAXDRW','TMOTHASS','TMOTHTOT','TMOTRESRV','TMPLGRCUR','TMPLGRTLT','TMPMCASH','TMPMEND','TMPMINV','TMPMOTH','TMPPE','TMRESRVTOT','TMTMCASH','TMTMEND','TMTMINV','TMTMOTH','TMTOTASS','TOATNDCD','TOATNDSF','TOCLIAOT','TOCURASS','TOCURINV','TODEFREV','TOLTINV','TOLTLIAAG','TOLTLIAOT','TONASSCURSF','TONASSLTSF','TONASSAG','TOOTHASS','TOPERFCD','TOPMEND','TOSHOWCD','TOT1099FM','TOT1099SF','TOTACEXP','TOTACEXPAG','TOTARECVCD','TOTASSAG','TOTASSETS','TOTBLDG','TOTCAPCD','TOTCMP','TOTCNTOPTO','TOTCNTTO','TOTCURCD','TOTDEVCD','TOTEXSF','TOTEXTOX','TOTGALF','TOTIMPRV','TOTLAND','TOTLIAAG','TOTLIACD','TOTLIAPM','TOTLIATM','TOTLIATOT','TOTLIAUN','TOTLOANSCD','TOTMEND','TOTPMIN','TOTPMX','TOTPRG','TOTSALCD','TOTSUBF','TOTSUBP','TOTTMIN','TOTTMX','TOTUNIN','TOTUNX','TOURISTSN','TOURREVCD','TRAVHTDEV','TRAVHTGA','TRAVHTPRG','TRAVHTSF','TRAVHTTOT','TREXSHOW','TRIBAL','TRIBALPM','TRIBALTM','TRIBALTO','TRIBNARCD','TRIBNCD','TRNCSTCD','TRNCSTWC','TRSNARCD','TRUSTCD','TRUSTNCD','TRUSTPM','TRUSTSF','TRUSTTM','TRUSTTO','TTEARNSF','TTNVSTCD','TTNVSTPM','TTNVSTTM','TTNVSTTO','TWFLWRS','UANARCD','UAOCDNEW','UAOPMNEW','UAOSF','UAOTMNEW','UAOTONEW','UNACEXP','UNARECVCD','UNBDEND','UNCARESRV','UNCASH','UNCLIAOT','UNCURASS','UNCURINV','UNCURLIA','UNDEFREV','UNIFBALA','UNIFBALL','UNINVRESRV','UNLTASS','UNLTINV','UNLTLIAOT','UNOTHASS','UNOTRESRV','UNPLGRCUR','UNPLGRTLT','UNPMCASH','UNPMEND','UNPMINV','UNPMOTH','UNPPE','UNRESRVTOT','UNTMCASH','UNTMEND','UNTMINV','UNTMOTH','UNTOTASS','UWEBVIS','VACTSAT','VATTTOT','VCNTART','VCOMCNSGN','VFATND','VISARTTOT','VMFLWRS','VPTOATND','WEBVIEWS','WEBVISITS','WKSDEVEL','WKSRDG','WorldCD','WORLDEXH','WORLDPRF','XFERS','XFERSPM','XFERSTM','XFERSTO','YRINC','YOUTFLWRS']
"""

numfields=['ADMISS','ADMISSRS','ADMISSTO','ADULTATND','ADVERTCD','ADVERTRS','ADVERTTO','ADVOCENG','ADVPRDEV','ADVPRGA','ADVPRPRG','ADVPRSF','ADVPRTOT','AFTA','ALLEDFR','ALLEDPD','APPFEECD','ARAG','ARTSALCD','AUDIT','BDCASH','BDCORPUS','BDENDDRW','BDENDINV','BDMAXDRW','BDOTH','BENSF','BENTOT','BOARDCD','BOARDHRS','BONDPYCUR','BONDPYLT','CAPEXPTOT','CAPPM','CAPTO','CAPUN','CASHAG','CASHCD','CASRESRVTOT','CDPTAXON','CITYPM','CITYCD','CITYNARCD','CITYNCD','CITYSF','CITYTO','CLIAOTAG','CLMGCNDEV','CLMGCNGA','CLMGCNPRG','CLMGCNTOT','CNCESSCD','CNCESSRS','CNCESSTO','CNFHSPDEV','CNFHSPGA','CNFHSPPRG','CNFHSPSF','CNFHSPTOT','CNTART','CNTPMCD','CNTOTPMCD','CNTOTTOCD','CNTOTUNCD','CNTYNARCD','COMCNSGN','CONSTRUCT','CONSULTHIFEE','CONSULTLOFEE','CONSULTN','CONSVC','CONSVCRS','CONSVCTO','CORPCD','CORPNCD','CORPPM','CORPSF','CORPTO','COUNTYCD','COUNTYNCD','COUNTYPM','COUNTYSF','COUNTYTO','CRLNEOY','CRLNLIM','CRPNARCD','CURASSAG','CURLIAAG','CYEAR','CYRFOUND','CZIP','CZIP4','DEFREVAG','DEPRCNCD','DEVBENCD','DEVCONTO','DEVSALCD','DIGADS','DIGSHOW','DMAILN','DPRCNDEV','DPRCNGA','DPRCNPRG','DPRCNSF','DPRCNTO','DUESDEV','DUESGA','DUESPRG','DUESSF','DUESTOT','DUNS','ED3INCCDN','ED3INCRS','ED3INCTON','EDPERFS','EDPUBPROF','EDSERIES','EDSERPERFS','EIN','EMAILN','EQUIPCD','ERNNCAPCD','EXHEXPDEV','EXHEXPGA','EXHEXPPRG','EXHEXPTOT','EXHWKSTOT','FBFLWRS','FEDCD','FEDNARCD','FEDNCD','FEDPM','FEDSF','FEDTO','FESTATTOT','FESTSHOW','FISCSPEIN','FILMS','FILMSCRN','FIXDASSAG','FIXDASSCD','FIXDASSPM','FIXDASSUN','FLKFLWRS','FNDNARCD','FOUNDCD','FOUNDNCD','FOUNDPM','FOUNDSF','FOUNDTO','FRATNDCD','FRATNDSF','FSAFCD','FSAFRS','FSAFTO','FTACARCD','FTARTPER','FTEMPS','FTSEAS','FTSEASFTE','FTVOLS','FYEND','GA1099','GABENCD','GALSALTM','GALSALTO','GALSALUN','GASAL','GDTOURRS','GDTOURTO','GDTOURUN','GFTSLS','GFTSLSRS','GFTSLSTO','GOVCD','GOVN','GOVPM','GOVTO','GPFLWRS','GRANTDEV','GRANTGA','GRANTPRG','GRANTSF','GRANTTOT','GRPINRS','GRPINTO','GRPINUN','GUIDTPERF','GUIDTSHOW','HASADVO','HASAFTA','HASAHC','HASARTSED','HASAUDIT','HASBCAST','HASBHS','HASCOMM','HASCOMP','HASCONF','HASDEVS','HASENDW','HASEXHS','HASFESTS','HASFS','HASGRNTS','HASLAO','HASLF','HASMEDSUB','HASMEMBR','HASNGLD','HASNONOPE','HASNONOPR','HASOPAM','HASOPREH','HASPERFS','HASPUB','HASRESIDENC','HASRESRCH','HASRSREV','HASSCREENS','HASSF','HASSUBEVS','HASTOURS','HISUBCD','HITIXMUS','HITIXPERF','HITXIMEMB','HITXOMEMB','ICART','ICTOT','IGFLWRS','IKB','IKCAO','IKCE','IKCIP','IKL','IKLBI','IKO','INDMEMLPSN','NewMembs','INKIND','INKINDOPPM','INKINDOPTO','INKINDOPUN','INKINDPM','INKINDSF','INKINDTM','INKINDTO','INKNDNOPPM','INKNDNOPTO','INKNDNOPUN','INSURCD','INSURDEV','INSURGA','INSURPRG','INSURSF','INTEXDEV','INTEXGA','INTEXPRG','INTEXSF','INTEXTOT','INTFXDASS','INTNTOT','INVAG','INVRESRVTOT','IRSDATE','KIDATTND','KIDATTPSF','KIDSINSCHLS','LECOCC','LOANINCD','LOANINRS','LOANINTO','LOANSCURAG','LOANSCURPM','LOANSCURTO','LOANSCURUN','LOANSLTAG','LOANSLTPM','LOANSLTTO','LOANSLTUR','LOCART','LOCARTSF','LOCARTTOT','LOCPREM','LOCPREMEXH','LOCPREMPRF','LOSUBCD','LOTIXIMEMB','LOTIXMUS','LOTIXOMEMB','LOTIXPERF','LTASSAG','LTASSSF','LTINVAG','LTLIAAG','LTLIACD','LTLIAPM','LTLIAUN','MBRINC','MBRINCTO','MBRINDFREN','MBRINDTOTN','MBRINRS','MBRORGFRN','MBRORGPDN','MBRORGTOTN','MBROTHFRN','MBROTHPDN','MBROTHTOTN','MEMBERS','MEMRETRNN','MKTPFO','MKTSAT','MKTTOT','MKTTOTSF','MOCOUNT','MORTCUR','MORTLT','NARTRCD','NARTRCDX','NARTRPM','NARTRTO','NATPREM','NATPRMEXH','NATPRMPRF','NETINCCD','NETINCNOP','NETINCOP','NETINCSF','NETINCUN','NewSubs','NISPID','NLOCART','NLOCARTSF','NLOCARTTOT','NOAUDITAG','NOCOMMISS','NOLECTUR','NONOPEXPTOT','NONOPSURP','NOOTPRG','NOTOPPM','NOTOPTO','NOTOPUN','NOTOPXFER','NOTOPXFERPM','NOTOPXFERTM','NOTOPXFERTO','NPERSEXDEV','NPERSEXGA','NPERSEXPRG','NPERSEXTOT','NVSTINOPCD','NVSTINOPPM','NVSTINOPTO','NVSTINOTCD','NVSTINOTPM','NVSTINOTTO','OACTSAT','OCCDEV','OCCGA','OCCPROG','OCCSF','OCCTOT','OCNTART','OCOMCNSGN','OFFCDEV','OFFCGA','OFFCPRG','OFFCTOT','OFFEDSHW','OFFSF','OFXDASS','ONONOPEXDEV','ONONOPEXGA','ONONOPEXPRG','OOPEXDEV','OOPEXGA','OOPEXPRG','OOPEXSF','OOPEXTOT','OPEARNPM','OPEXPTOT','OPPM','OPREHTOT','OPRGRVCD','OPRGRVTM','OPRGRVTO','OPSF','OPSURPCDD','OPTO','OPUN','ORGID','ORGLAT','ORGLONG','ORGMEMLPSN','ORGMEMNEWN','ORGMEMRETRN','OrgYrFound','OSMFLWRS','OTARTTOT','OTCURASAG','OTDON','OTDONPM','OTDONTO','OTHEARNCD','OTHEARNRS','OTHEARNSF','OTHEARNTO','OTHERMKTG','OTHINDCD','OTHINDPM','OTHINDSF','OTHINDTO','OTHLNCUR','OTHLNLT','OTHMEMLPSN','OTHMEMNEWN','OTMEMRETRNN','OTHNARCD','OTHRENTRS','OTHRENTTO','OTHRENTUN','OTINDNCD','OTLTASSAG','OTPMCUR','OTPRGPRF','OTRESRVTOT','OTTOCUR','OTUNCUR','PACTSAT','PARK','PARKRS','PARKTO','PAYABAG','PAYABPM','PAYABTO','PAYABUN','PCNTART','PCOMCNSGN','PDACARCD','PDATNDADM','PDATNDPERF','PEIN','PERSNEXDEV','PERSNEXGA','PERSNEXPRG','PERSNEXSF','PFEEFUND','PFEEGA','PFEEPRG','PFEESF','PFEETOT','PHATTTOT','PINFLWRS','PLGRAG','PLGRCURTOT','PLGRTLTAG','PLGRTLTTO','PMACEXP','PMARECVCD','PMCASH','PMCASHTO','PMCLIAOT','PMCORPUS','PMCURASS','PMCURINV','PMCURLIA','PMDEFREV','PMENDDRW','PMEXSHOW','PMIFBALA','PMIFBALL','PMINVTO','PMLTASS','PMLTINV','PMLTLIAOT','PMMAXDRW','PMOTHASS','PMOTHTOT','PMPLGRCUR','PMPLGRTLT','PMPMCASH','PMPMEND','PMPMINV','PMPMOTH','PMPPE','PMTOTASS','PNOEXDEV','PNOEXGA','PNOEXPRG','PNOEXTOT','PPADJPM','PPADJTO','PPADJUN','PREPADAG','PREPADCD','PRFARTTOT','PRGBENCD','PRODEXDEV','PRODEXGA','PRODEXPRG','PRODEXTOT','PRRADTVADS','PRSFEERS','PRSFEVTTO','PRTSHDEV','PRTSHGA','PRTSHPRG','PRTSHSF','PRTSHTOT','PRVEXPERTS','PRVSPACE','PTARTPER','PTEMPS','PTFTES','PTSEAS','PTSEASFTE','PTVFTES','PTVOLS','PUBDIST','PUBPRINT','PUBSALTM','PUBSALTO','PUBSALUN','PVTLESSONS','PVTSTUDS','RECDEV','RECGA','RECPRG','RECTOT','RESRPTS','RESRVDRW','RESRVMAXDR','RESRVTOT','ROYALCD','ROYALRS','ROYALTO','RYLDEV','RYLEXPCD','RYLGA','RYLTOT','SALSF','SALTO','SENATTND','SHLTRCD','SHLTRYRFND','SHLTREIN','SMORG','SMEDFLWRS','SNONOPEXDV','SNONOPEXGA','SNONOPEXPR','SomeDate','SOPEXDEV','SOPEXGA','SOPEXPRG','SOPEXSF','SPC1SQFT','SPC1ZIP','SPC2SQFT','SPC2ZIP','SPC3SQFT','SPC3ZIP','SPC4SQFT','SPC4ZIP','SPC5SQFT','SPC5ZIP','SPCRENTRS','SPCRENTTO','SPCRENTUN','SPEVCRPN','SPEVGRPM','SPEVGRTO','SPEVGRUN','SPEVINDN','SPEVNETPM','SPEVNETTO','SPEVNETUN','SPEVOTHN','SPEVTOTN','SPNSOR','SPNSORRS','SPNSORTO','SPRENTSQFT','STANARCD','STATECD','STATENCD','STATEPM','STATESF','STATETO','STINCRS','STINCTO','STXINCUN','SUBATTF','SUBATTP','SUBBCSTCD','SUBBCSTRS','SUBBCSTTO','SUBINCFLRS','SUBINCFLTO','SUBINCFLX','SUBINCFUL','SUBINCFXRS','SUBINCFXTO','SUBMEDBFR','SUBMEDBPD','SUBMEDBTO','SUBMEDPFR','SUBMEDPPD','SUBMEDPTO','SUBPUBCD','SUBPUBRS','SUBPUBTO','SUBRETRNN','SUBSTATTOT','SUGDONAMT','TCNTCDOPSF','TKTADMFR','TKTADMPD','TKTADMTO','TMCARESRV','TMCASHTO','TMCORPUS','TMENDBSCHK','TMENDDRW','TMEXSHOW','TMFLWRS','TMINVRESRV','TMINVTO','TMMAXDRW','TMOTHTOT','TMOTRESRV','TMRESRVTOT','TMTOTASS','TOATNDCD','TOATNDSF','TOCLIAOT','TOCURASS','TOCURINV','TODEFREV','TOLTINV','TOLTLIAAG','TOLTLIAOT','TONASSCURSF','TONASSLTSF','TONASSAG','TOOTHASS','TOPERFCD','TOPMEND','TOSHOWCD','TOT1099FM','TOT1099SF','TOTACEXP','TOTACEXPAG','TOTARECVCD','TOTASSAG','TOTASSETS','TOTBLDG','TOTCAPCD','TOTCMP','TOTCNTOPTO','TOTCNTTO','TOTCURCD','TOTDEVCD','TOTEXSF','TOTEXTOX','TOTGALF','TOTIMPRV','TOTLAND','TOTLIAAG','TOTLIACD','TOTLIAPM','TOTLIATM','TOTLIATOT','TOTLIAUN','TOTLOANSCD','TOTMEND','TOTPMIN','TOTPMX','TOTPRG','TOTSALCD','TOTSUBF','TOTSUBP','TOTTMIN','TOTTMX','TOTUNIN','TOTUNX','TOURISTSN','TOURREVCD','TRAVHTDEV','TRAVHTGA','TRAVHTPRG','TRAVHTSF','TRAVHTTOT','TREXSHOW','TRIBAL','TRIBALPM','TRIBALTO','TRIBNARCD','TRIBNCD','TRNCSTCD','TRNCSTWC','TRSNARCD','TRUSTCD','TRUSTNCD','TRUSTPM','TRUSTSF','TRUSTTO','TTEARNSF','TTNVSTCD','TTNVSTPM','TTNVSTTM','TTNVSTTO','TWFLWRS','UANARCD','UAOCDNEW','UAOPMNEW','UAOSF','UAOTONEW','UNACEXP','UNARECVCD','UNBDEND','UNCARESRV','UNCASH','UNCLIAOT','UNCURASS','UNCURINV','UNCURLIA','UNDEFREV','UNIFBALA','UNIFBALL','UNINVRESRV','UNLTASS','UNLTINV','UNLTLIAOT','UNOTHASS','UNOTRESRV','UNPLGRCUR','UNPLGRTLT','UNPMCASH','UNPMEND','UNPMINV','UNPMOTH','UNPPE','UNRESRVTOT','UNTOTASS','UWEBVIS','VACTSAT','VATTTOT','VCNTART','VCOMCNSGN','VFATND','VISARTTOT','VMFLWRS','VPTOATND','WEBVIEWS','WEBVISITS','WKSDEVEL','WKSRDG','WorldCD','WORLDEXH','WORLDPRF','XFERS','XFERSPM','XFERSTO','YRINC','YOUTFLWRS']

#print("Total Numeric fields: ", len(numfields)

def parse_cdp_df(rawdf):
    #- replace '2020-04-20' --> 202004 format
    #rawdf['CFYEND']=rawdf['CFYEND'].str.replace('-','').str[:-2] 
    dt_fields=['CFYEND','SomeDate','FYEND']
    print("Parsing dates fields: ",dt_fields)
    rawdf=parse_dt(rawdf,dt_fields)
    #- replacing '-' in the fields
    print("Parsing fields for unnecessary dashes")
    rawdf=remove_dash(rawdf,'EIN')
    rawdf=remove_dash(rawdf,'PEIN')
    rawdf=remove_dash(rawdf,'DUNS')
    #- remove hard carriage returns
    rawdf.replace('\r\n','',regex=True)
    #- assign to the numeric fields
    print("Assigning numeric values to total fields: ", len(numfields))
    rawdf=assign_numeric(rawdf,numfields)
    #- remove cdpname examples/sample
    print("Removing unreal-- examples,samples records entirely")
    for val in ['Example','example','sample','Sample']:
        rawdf=remove_examples(rawdf,'CDPNAME',val)
    print("Parsed df shape", rawdf.shape)
    return rawdf

def parse_dt(indf, fields):
    df=indf.copy()
    for field in fields:
        df[field]=df[field].astype(str)
        df[field]=df[field].str.replace('-','').str[:6]
        df[field]=df[field].replace("nan",np.nan,regex=True)
    return df

def remove_dash(inputdf,field):
    #df[field].fillna("nan",inplace=True)
    df=inputdf.copy()
    df[field]=df[field].astype(str)
    df[field]=df[field].str.replace('-','',regex=False).str.replace('.0','',regex=False)
    df[field]=df[field].replace("nan",np.nan,regex=True).str.replace(' ','')
    return df

def assign_numeric(indf,fields):
    df=indf.copy()
    nofields=[]
    yesfields=[]
    for field in fields:
        if field not in df.columns.values:
            nofields.append(field)
        else:
            yesfields.append(field)
            df[field]=pd.to_numeric(df[field],errors='coerce',downcast='integer')
    print("Found fields total: ",len(yesfields))
    print("Missed fields total: ", len(nofields))
    print("All missed fields: ", nofields)
    return df

def remove_examples(indf,field,value):
    df=indf.copy()
    df[field]=df[field].astype(str)
    df=df[~df[field].str.contains(value)].reset_index(drop=True)
    df[field]=df[field].replace("nan",np.nan,regex=True)
    return df
    
inputcdp=pd.read_csv(args.input_csv)
#inputcdp.rename(str.upper,axis='columns')
#inputcdp['ORGID']=inputcdp['ORGSID']
#inputcdp=inputcdp.drop(['ORGSID'],axis=1)
print("Input file shape: ", inputcdp.shape)
parsedcdp=parse_cdp_df(inputcdp)
parsedcdp['SomeDate']=pd.to_numeric(parsedcdp['SomeDate'],errors='coerce',downcast='integer')
cdpfinal=parsedcdp[parsedcdp['SomeDate']>=202001]
print("Final df shape: ", cdpfinal.shape)
print("writing output to ", args.output_csv)
cdpfinal.to_csv(args.output_csv,index=False)

