// Copyright (c) 2012-2016, The CryptoNote developers, The Bytecoin developers
// Copyright (c) 2016-2021, The Karbo developers

#ifndef MISC_MES_H
#define MISC_MES_H

#include <cstdint>
#include <iostream>

#include "Common/ConsoleTools.h"

namespace concolor
{
    using namespace Common::Console;

    inline std::basic_ostream<char, std::char_traits<char> >& red(std::basic_ostream<char, std::char_traits<char> >& ostr)
    {
        setTextColor(Color::BrightRed);
        return ostr;
    }
}

#ifndef CHECK_AND_ASSERT_MES
#define CHECK_AND_ASSERT_MES(expr, fail_ret_val, message)   do{if(!(expr)) {std::cout << concolor::red << message << concolor::normal << std::endl; return fail_ret_val;};}while(0)
#endif

#endif  /* MISC_MES_H */