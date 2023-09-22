import './globals.css';
import { Inter } from 'next/font/google';

const inter = Inter({ subsets: ['latin'] });

export const metadata = {
  title: 'Coral',
  description:
    'About\n' +
    'Coral is a translation, analysis, and query rewrite engine for SQL and other relational languages.',
};

export default function RootLayout({ children }) {
  return (
    <html lang='en' className='h-full bg-white'>
      <body className={`h-full ${inter.className} font-courier`}>
        {children}
      </body>
    </html>
  );
}
