# pytroll-cspp-runner

Pytroll package that runs the Community Satellite Processing Package
(CSPP) Sensor Data Record (SDR) software package by the Cooperative
Institute for Meteorological Satellite Studies (CIMMS) at the University
of Wisconsin-Madison.  CSPP-SDR converts Raw Data Records (RDRs)
to SDRs for the Very Interesting Infrared Radiation Sensor (VIIRS).
The cspp-runner encapsulates this in a Pytroll chain.  It will listen
on the nameserver to posttroll messages informing of arriving RDR
files (those messages might come from trollstalker, trollmoves, or the
geographic gatherer), run the CSPP SDR software, ensure that ancillary
data and lookup tables are regularly updated (such as required by this
software), and send another posttroll message upon completion (which
might be listened to by trollflow2).

Example configuration files are located in the examples directory.  CSPP SDR
must be installed.  Additionally, the environment variables `CSPP_SDR_HOME` and
`CSPP_WORKDIR` must be set.

More information about CSPP can be found at https://cimss.ssec.wisc.edu/cspp/.
