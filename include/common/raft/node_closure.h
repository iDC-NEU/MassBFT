//
// Created by peng on 2/11/23.
//

#ifndef NBP_NODE_CLOSURE_H
#define NBP_NODE_CLOSURE_H

#include "braft/node.h"

namespace util::raft {
    class ExpectClosure : public braft::Closure {
    public:
        void Run() override {
            if (_expect_err_code >= 0) {
                ASSERT_EQ(status().error_code(), _expect_err_code) << _pos << " : " << status();
            }
            if (_cond) {
                _cond->signal();
            }
            delete this;
        }

        ExpectClosure(bthread::CountdownEvent *cond, int expect_err_code, const char *pos)
                : _cond(cond), _expect_err_code(expect_err_code), _pos(pos) {}

        ExpectClosure(bthread::CountdownEvent *cond, const char *pos)
                : _cond(cond), _expect_err_code(-1), _pos(pos) {}

        ~ExpectClosure() override = default;

    private:
        bthread::CountdownEvent* _cond;
        int _expect_err_code;
        const char *_pos;
    };
}

#define NEW_CLOSURE(arg) (new util::raft::ExpectClosure((arg), __FILE__ ":" BAIDU_SYMBOLSTR(__LINE__)))
#define NEW_CLOSURE_WITH_CODE(arg, code) (new util::raft::ExpectClosure((arg), code, __FILE__ ":" BAIDU_SYMBOLSTR(__LINE__)))

#endif //NBP_NODE_CLOSURE_H
