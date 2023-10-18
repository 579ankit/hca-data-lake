FROM gcr.io/dataflow-templates-base/python39-template-launcher-base

  ARG WORKDIR=/dataflow/template
  RUN mkdir -p ${WORKDIR}
  WORKDIR ${WORKDIR}

#   COPY requirements.txt and .py files
  COPY pubsubtogcs.py .

  # Do not include `apache-beam` in requirements.txt
  # ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="${WORKDIR}/requirements.txt"
  ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/pubsubtogcs.py"

  # Install apache-beam and other dependencies to launch the pipeline
  RUN pip install apache-beam[gcp]==2.50.0
#   RUN pip install -U -r ./requirements.txt