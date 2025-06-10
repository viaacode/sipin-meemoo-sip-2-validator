FROM python:3.12-slim

# Install OpenJDK-17 and wget
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk-headless wget && \
    apt-get clean;

# Set Java path.
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64/
ENV PATH $JAVA_HOME/bin:$PATH

# Make a new group and user so we don't run as root.
RUN addgroup --system appgroup && adduser --system appuser --ingroup appgroup

WORKDIR /app

# Let the appuser own the files so he can rwx during runtime.
COPY --chown=appuser:appgroup . .
# COPY . .

# We install all our Python dependencies. Add the extra index url because some
# packages are in the meemoo repo.
RUN pip install -e . \
    --extra-index-url http://do-prd-mvn-01.do.viaa.be:8081/repository/pypi-all/simple \
    --trusted-host do-prd-mvn-01.do.viaa.be

USER appuser

# This command will be run when starting the container. It is the same one that can be used to run the application locally.
ENTRYPOINT [ "python", "main.py"]