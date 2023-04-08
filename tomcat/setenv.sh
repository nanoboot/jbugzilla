# place in /bin directory of Tomcat installation

JBUGZILLA_CONFPATH="{path to confpath directory}"

export JAVA_OPTS="$JAVA_OPTS -Djbugzilla.confpath=${JBUGZILLA_CONFPATH}"

