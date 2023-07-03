//
// Created by user on 23-7-3.
//

#include "chaincode.h"

namespace peer::chaincode {
    class YCSBRowLevel : public Chaincode {
    public:
        explicit YCSBRowLevel(std::unique_ptr<ORM> orm) : Chaincode(std::move(orm)) { }

        int InvokeChaincode(std::string_view funcNameSV, std::string_view argSV) override;

        int InitDatabase() override;

    protected:
        int update(std::string_view argSV);

        int insert(std::string_view argSV);

        int read(std::string_view argSV);

        int remove(std::string_view argSV);

        int scan(std::string_view argSV);

        int readModifyWrite(std::string_view argSV);

    private:
        int updateValue(std::string_view keySV, const std::vector<std::pair<std::string_view, std::string_view>>& rhs);

        int modifyValue(std::string_view keySV,
                        const std::vector<std::string_view>& reads,
                        const std::vector<std::pair<std::string_view, std::string_view>>& rhs);

        int insertValue(std::string_view keySV, const std::vector<std::pair<std::string_view, std::string_view>>& rhs);
    };
}
