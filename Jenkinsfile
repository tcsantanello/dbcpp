
conan_build(
  agent:     [ names: [ 'postgres' ] ],
  compilers: [ 'gcc5', 'gcc6', 'gcc7', 'gcc8' ],
  environment: [
    POSTGRESQL_USERNAME: 'testdb',
    POSTGRESQL_DATABASE: 'testdb',
    POSTGRESQL_PASSWORD: '123456',
    POSTGRESQL_HOSTNAME: 'localhost',
    CONAN_BUILD_LIBCXX:  'libstdc++11',
  ]
)
