/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    "./src/**/*.{html,ts}",
  ],
  theme: {
    extend: {
      colors: {
        navy: {
          50: '#E8EAF6',
          600: '#1B2559',
          700: '#161e47',
        },
        teal: {
          500: '#00BCD4',
          600: '#00ACC1',
        }
      }
    },
  },
  plugins: [],
}
