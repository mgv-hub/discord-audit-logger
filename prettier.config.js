export default {
    tabWidth: 4,
    useTabs: false,
    singleQuote: true,
    semi: true,
    trailingComma: 'all',
    printWidth: 90,
    arrowParens: 'always',
    endOfLine: 'lf',
    overrides: [
        {
            files: ['*.txt', '*.md', '*.bat', '*.sh'],
            options: {
                tabWidth: 4,
                printWidth: 100,
                singleQuote: true,
                semi: true,
            },
        },
    ],
};
