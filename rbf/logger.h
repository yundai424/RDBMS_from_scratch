#ifndef CS222_PROJ1_LOGGER_H
#define CS222_PROJ1_LOGGER_H


//#define RELEASE

#include <utility>
#include <iostream>
#include <iomanip>
#include <map>
#include <vector>
#include <ostream>
#include <stdexcept>

using namespace std;

enum LogLevel {
    DEBUGGING = 0,
    INFO = 1,
    WARNING = 2,
    ERROR = 3,
    QUIET = 4

};
static map<LogLevel, string> kPrefixMap = {
        {DEBUGGING,   "\e[1;96m[TRACE]"},
        {INFO,    "\e[1;32m[INFO]"},
        {WARNING, "\e[1;33m[WARN]"},
        {ERROR,   "\e[1;31m[ERROR]"},
};
static string kPostfix = "\e[0m";

class Logger {
private:
    LogLevel level_;
    string file_path_;
    string func_path_;
    int line_num_;
    bool opened_;
    static LogLevel global_level;

    inline static string get_file_name(const string &path) {
        return path.substr(path.find_last_of("/\\") + 1);
    }

public:
    Logger() = delete;

    Logger(const Logger &) = delete;

    explicit Logger(LogLevel level, string file_path, int line_num, string func_path)
            : level_(level), file_path_(move(file_path)), line_num_(line_num), func_path_(move(func_path)),
              opened_(false) {
        cout << boolalpha;
    }

    ~Logger() {
        if (opened_) cout << endl;
    }

    template<typename T>
    Logger &operator<<(const T &v) {
        if (level_ < global_level) return *this;
        if (!opened_) {
            file_path_ = get_file_name(file_path_);
            cout << setw(14) << left << dec << kPrefixMap[level_]
                      << " In '" << func_path_ << "' " << file_path_ << ":" << line_num_
                      << " " << kPostfix;
            opened_ = true;
        }
        cout << v;
        return *this;
    }

    static void SetGlobalLogLevel(LogLevel level) { global_level = level; }

    static LogLevel GetGlobalLogLevel() { return global_level; }
};


template<typename T>
ostream &operator<<(ostream &os, const vector<T> &v) {
    os << "[";
    for (int i = 0; i < v.size(); ++i) {
        os << v[i];
        if (i != v.size() - 1) os << ", ";
    }
    os << "]\n";
    return os;
}

#define DB_TRACE Logger(LogLevel::TRACE, __FILE__, __LINE__, __FUNCTION__)
#define DB_DEBUG Logger(LogLevel::DEBUGGING, __FILE__, __LINE__, __FUNCTION__)
#define DB_INFO Logger(::LogLevel::INFO, __FILE__, __LINE__, __FUNCTION__)
#define DB_WARNING Logger(::LogLevel::WARNING, __FILE__, __LINE__, __FUNCTION__)
#define DB_ERROR Logger(::LogLevel::ERROR, __FILE__, __LINE__, __FUNCTION__)


#endif //CS222_PROJ1_LOGGER_H
