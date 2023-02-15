//
// Created by peng on 2/15/23.
//

#ifndef NBP_MATRIX_2D_H
#define NBP_MATRIX_2D_H

#include <vector>
namespace util {
    template <class T>
    class Matrix2D {
    public:
        explicit Matrix2D(int x=0, int y=0) : data(x*y), columns(x), rows(y) {}
        Matrix2D(Matrix2D&& rhs)  noexcept : data(std::move(rhs.data)), columns(rhs.columns), rows(rhs.rows) {}

        Matrix2D(const Matrix2D&) = delete;
        bool operator=(const Matrix2D&) = delete;

        void reset(int x, int y) {
            data = std::vector<T>(x*y);
            columns = x;
            rows = y;
        }

        T &operator()(int x, int y) {
            return data[y * columns + x];
        }

        const T &operator()(int x, int y) const {
            return data[y * columns + x];
        }

        [[nodiscard]] int x() const { return columns; }

        [[nodiscard]] int y() const { return rows; }

    private:
        std::vector<T> data;
        int columns;
        int rows;
    };
}
#endif //NBP_MATRIX_2D_H
