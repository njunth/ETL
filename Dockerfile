FROM registry.njuics.cn/fluentd/python:3.5
ADD ./* /root/data_process/
WORKDIR /root/data_process/
RUN pip install -r requirements.txt
ENTRYPOINT ["python"]