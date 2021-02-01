#ifndef HAVE_CONFIG_H
#  define HAVE_CONFIG_H /* Force using config.h, so test would fail if header
                           actually tries to use it */
#endif
#include <ucp/api/ucp.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/epoll.h>
#include <netinet/in.h>
#include <assert.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>  /* getopt */
#include <ctype.h>   /* isprint */
#include <pthread.h> /* pthread_self */
#include <errno.h>   /* errno */
#include <time.h>
#include <signal.h>  /* raise */
#include <arpa/inet.h>
#include<iostream>
#include <stdlib.h>
#include <stdio.h>
#include <sys/time.h>
#include <unistd.h>

int64_t start = 0;
int64_t end = 0;
bool is_server = true;
int64_t get_current_time() {
    struct timeval tv;
    gettimeofday(&tv,NULL);
    return tv.tv_sec * 1000 * 1000 + tv.tv_usec;
}

struct TestReq {
    bool _completed;
    char *_buf;
};

struct EchoReq {
    char _data[16];
};

struct EchoRsp {
    char _data[16];
};

static void request_init(void *request) {
    struct TestReq *req = (struct TestReq *)request;
    req->_completed = false;
}

struct AcceptCtx {
    ucp_context_h _ucp_context;
    ucp_worker_h _ucp_worker;
    ucp_ep_h     _ucp_ep;
};

static void accept_callback(ucp_conn_request_h conn_request, void *arg) {
    AcceptCtx *ctx = (AcceptCtx *)arg;
    ucp_ep_params_t ep_params;

    ep_params.field_mask   = UCP_EP_PARAM_FIELD_CONN_REQUEST;
    ep_params.conn_request = conn_request;
    ucs_status_t status = ucp_ep_create(ctx->_ucp_worker, &ep_params, &ctx->_ucp_ep);
    if (UCS_OK != status) {
        printf("ucp_ep_create failed. status:%d\n", status);
    } else {
        printf("ucp_ep_create succeed.\n");
        ucp_ep_print_info(ctx->_ucp_ep, stdout);
    }
}

void recv_cb(void *request, ucs_status_t status, ucp_tag_recv_info_t *info) {
    if (UCS_OK == status) {
        if (!is_server) {
            end = get_current_time();
            printf("cost:%ldus\n", end - start);
        }
        //printf("receive req succeed\n");
        TestReq *req = (TestReq *)request;
        req->_completed = true;
        //printf("buf[0]=%c\n", req->_buf[0]);
    } else {
        printf("receive req failed. status:%d\n", status);
    }
}

void send_cb(void *request, ucs_status_t status) {
    if (UCS_OK == status) {
        //printf("send req succeed\n");
    } else {
        printf("send req failed. status:%d\n", status);
    }
    ucp_request_free(request);
}

void run_test_server(ucp_context_h &ucp_context, ucp_worker_h &ucp_worker, int port) {
    ucs_status_t status;
    
    // create listener
    AcceptCtx ctx;
    ctx._ucp_context = ucp_context;
    ctx._ucp_worker = ucp_worker;
    ucp_listener_params_t listener_params;
    struct sockaddr_in sockaddr;
    memset(&sockaddr, 0, sizeof(sockaddr));
    sockaddr.sin_family = AF_INET;
    sockaddr.sin_addr.s_addr = INADDR_ANY;
    sockaddr.sin_port = htons(port);
    listener_params.field_mask         = UCP_LISTENER_PARAM_FIELD_SOCK_ADDR |
        UCP_LISTENER_PARAM_FIELD_CONN_HANDLER;
    listener_params.sockaddr.addr      = (struct sockaddr *)&sockaddr;
    listener_params.sockaddr.addrlen   = sizeof(sockaddr);
    listener_params.conn_handler.cb    = accept_callback;
    listener_params.conn_handler.arg   = &ctx;
    ucp_listener_h listener;
    status = ucp_listener_create(ucp_worker, &listener_params, &listener);
    if (UCS_OK != status) {
        printf("upc_listener_create failed. status:%d\n", status);
        return;
    }
    printf("ucp_listener_create succeed\n");
    
    TestReq *recv_req = NULL; 
    char buffer[16];
    memset(buffer, 0, sizeof(buffer));
    ucs_status_ptr_t request = ucp_tag_recv_nb(ucp_worker, buffer, sizeof(buffer), ucp_dt_make_contig(1), 0, UINT64_MAX, recv_cb);
    if (UCS_PTR_IS_ERR(request)) {
        printf("ucp_tag_recv_nb failed.\n");
    } else {
        recv_req = (TestReq *)request;
        recv_req->_completed = false;
        recv_req->_buf = buffer;
    }
    EchoReq req;
    memset(req._data, 'c', sizeof(req._data));
    while (true) {
        ucp_worker_progress(ucp_worker);
        if (NULL != recv_req && recv_req->_completed) {
            // send response
            void *request = ucp_tag_send_nb(ctx._ucp_ep, &req, sizeof(req), ucp_dt_make_contig(1), 0, send_cb);
            if (NULL == request) {
                // completed
                //printf("send echo response succeed\n");
            } else if (UCS_PTR_IS_ERR(request)) {
                printf("send echo response failed\n");
            } else {
                //printf("send not completed, will finish async\n");
            }
            ucp_request_free(recv_req);
            recv_req = NULL;
        }
    }
}

void run_test_client(ucp_context_h &ucp_context, ucp_worker_h &ucp_worker, const char *ip, int port) {
    ucp_ep_params_t ep_params;
    ucp_ep_h ucp_ep;
    ep_params.field_mask   = UCP_EP_PARAM_FIELD_SOCK_ADDR | UCP_EP_PARAM_FIELD_FLAGS;

    struct sockaddr_in sockaddr;
    memset(&sockaddr, 0, sizeof(sockaddr));
    sockaddr.sin_family = AF_INET;
    sockaddr.sin_addr.s_addr = inet_addr(ip);
    sockaddr.sin_port = htons(port);
    ep_params.sockaddr.addr      = (struct sockaddr *)&sockaddr;
    ep_params.sockaddr.addrlen   = sizeof(sockaddr);
    ep_params.flags = UCP_EP_PARAMS_FLAGS_CLIENT_SERVER;
    ucs_status_t status = ucp_ep_create(ucp_worker, &ep_params, &ucp_ep);
    if (UCS_OK != status) {
        printf("ucp_ep_create failed. status:%d\n", status);
    } else {
        printf("ucp_ep_create succeed.\n");
        ucp_ep_print_info(ucp_ep, stdout);
    }
    
    // post receive request
    char buffer[16];
    memset(buffer, 0, sizeof(buffer));
    ucs_status_ptr_t request = ucp_tag_recv_nb(ucp_worker, buffer, sizeof(buffer), ucp_dt_make_contig(1), 0, UINT64_MAX, recv_cb);
    if (UCS_PTR_IS_ERR(request)) {
        printf("ucp_tag_recv_nb failed.\n");
    } else {
        TestReq *req = (TestReq *)request;
        req->_completed = false;
        req->_buf = buffer;
    }
    
    for (int i = 0; i < 5; i++) {
        ucp_worker_progress(ucp_worker);
        sleep(1);
    }

    // send test data
    EchoReq req;
    memset(req._data, 'a', sizeof(req._data));
    start = get_current_time(); 
    void *send_request = ucp_tag_send_nb(ucp_ep, &req, sizeof(req), ucp_dt_make_contig(1), 0, send_cb);
    if (NULL == send_request) {
        // completed
        //printf("send echo req succeed\n");
    } else if (UCS_PTR_IS_ERR(send_request)) {
        printf("send echo req failed\n");
    } else {
        //printf("send not completed, will finish async\n");
    }
    while (true) {
        ucp_worker_progress(ucp_worker);
    }
}

int main(int argc, char *argv[]) {
    int ret = 0;
    if (argc < 2) {
        printf("usage: ./test_ucp 0|1\n");
        printf("usage: 0 means server, 1 means client\n");
        return 1;
    }
    int mode = atoi(argv[1]);
    ucs_status_t status;

    // read ucp config and print
    ucp_config_t *config = NULL;
    status = ucp_config_read(NULL, NULL, &config);
    if (UCS_OK != status) {
        printf("ucp_config_read failed. status:%d\n", status);
        return 1;
    }
    ucp_config_print(config, stdout, NULL, UCS_CONFIG_PRINT_CONFIG);

    // ucp init
    ucp_params_t ucp_params;
    memset(&ucp_params, 0, sizeof(ucp_params));
    ucp_params.field_mask = UCP_PARAM_FIELD_FEATURES |
                            UCP_PARAM_FIELD_REQUEST_SIZE |
                            UCP_PARAM_FIELD_REQUEST_INIT;
    ucp_params.features = UCP_FEATURE_TAG;
    // using polling mode, no need to set wakeup feature
    ucp_params.request_size = sizeof(TestReq);
    ucp_params.request_init = request_init;
    ucp_context_h ucp_context;
    status = ucp_init(&ucp_params, config, &ucp_context);
    if (UCS_OK != status) {
        printf("ucp_init failed. status:%d\n", status);
        return 1;
    }
    printf("ucp_init succeed.\n");
    ucp_config_release(config);
    ucp_context_print_info(ucp_context, stdout);

    // ucp worker init
    ucp_worker_params_t worker_params;
    worker_params.field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
    worker_params.thread_mode = UCS_THREAD_MODE_SINGLE;
    ucp_worker_h ucp_worker;
    status = ucp_worker_create(ucp_context, &worker_params, &ucp_worker);
    if (UCS_OK != status) {
        printf("ucp_woker_create failed. status:%d\n", status);
        return 1;
    }
    printf("ucp_worker_create succeed.\n");

    if (0 == mode) {
        if (3 != argc) {
            printf("usage: ./test_ucp 0 port\n");
            return 1;
        }
        int port = atoi(argv[2]);
        is_server = true;
        run_test_server(ucp_context, ucp_worker, port);
    } else {
        if (4 != argc) {
            printf("usage: ./test_ucp 1 ip port\n");
            return 1;
        }
        is_server = false;
        const char *ip = argv[2];
        int port = atoi(argv[3]);
        run_test_client(ucp_context, ucp_worker, ip, port);
    }
    return ret;
}
