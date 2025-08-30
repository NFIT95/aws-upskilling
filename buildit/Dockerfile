# AWS Lambda Python base image
FROM public.ecr.aws/lambda/python:3.9

# Copy the function code and requirements file into the image
COPY lambda_function.py ${LAMBDA_TASK_ROOT}
COPY requirements.txt ${LAMBDA_TASK_ROOT}

# Install requirements
RUN pip install -r requirements.txt

# <filename>.<handler_function_name>
CMD [ "lambda_function.lambda_handler" ]
