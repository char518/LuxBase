/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{js,ts,jsx,tsx}"],
  theme: {
    extend: {
      colors: {
        canvas: "#f4f6fa",
        ink: "#172033",
        accent: "#176df5"
      },
      fontFamily: {
        sans: ["-apple-system", "BlinkMacSystemFont", "PingFang SC", "sans-serif"],
        mono: ["SFMono-Regular", "Menlo", "monospace"]
      }
    }
  },
  plugins: []
};
