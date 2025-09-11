FROM python:3.12-slim AS compile-image

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y git

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY . .
RUN pip install .

RUN echo $(ls /opt/venv/lib/python3.12/site-packages/amlaidatatests/resources/)

FROM python:3.12-slim  AS build-image
    
# Set up user environment without root permission
ARG USER=appuser
RUN useradd --create-home appuser

# Set non-root user
USER ${USER}

# Add installed package from compile image
COPY --from=compile-image --chown=${USER} /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

WORKDIR /home/${USER}

RUN echo $(ls /opt/venv/lib/python3.12/site-packages/amlaidatatests/resources/)

# Default
CMD [ "amlaidatatests", "--help" ]
