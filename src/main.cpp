#include "division/division.h"
#include "main.h"
#include <iostream>

using namespace std;

int main(int argc, const char *argv[]) {
  Fraction f;

  cout << HEADER;

  // ensure the correct number of parameters are used.
  if (argc < 3) {
    cout << USAGE;
    return 1;
  }

  f.numerator = atoll(argv[1]);
  f.denominator = atoll(argv[2]);

  Division d = Division(f);
  try {
    DivisionResult r = d.divide();

    cout << "Division : " << f.numerator << " / " << f.denominator << " = " << r.division << "\n";
    cout << "Remainder: " << f.numerator << " % " << f.denominator << " = " << r.remainder << "\n";
  } catch (DivisionByZero) {
    cout << "Can not divide by zero, Homer. Sober up!\n";
  }
  return 0;
}
