import { NextRequest, NextResponse } from 'next/server';
import { db } from '@/lib/db';
import bcrypt from 'bcryptjs';
import jwt from 'jsonwebtoken';
import { provisionUserWorkspace } from '@/lib/tenant';

const JWT_SECRET = process.env.JWT_SECRET || 'dataforge-jwt-secret-key-2024';

export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const { action, email, password, name } = body;

    if (action === 'signup') {
      if (!email || !password) {
        return NextResponse.json({ error: 'Email and password are required' }, { status: 400 });
      }
      if (password.length < 6) {
        return NextResponse.json({ error: 'Password must be at least 6 characters' }, { status: 400 });
      }

      const existingUser = await db.user.findUnique({ where: { email } });
      if (existingUser) {
        return NextResponse.json({ error: 'An account with this email already exists' }, { status: 400 });
      }

      const hashedPassword = await bcrypt.hash(password, 10);
      const user = await db.user.create({
        data: {
          email,
          name: name || email.split('@')[0],
          password: hashedPassword,
        },
      });

      const workspace = provisionUserWorkspace(user.id);
      const updated = await db.user.update({
        where: { id: user.id },
        data: {
          workspaceRoot: workspace.root,
          dbPath: workspace.dbPath,
        },
      });

      const token = jwt.sign({ userId: updated.id, email: updated.email }, JWT_SECRET, { expiresIn: '7d' });

      return NextResponse.json({
        success: true,
        token,
        user: {
          id: updated.id,
          email: updated.email,
          name: updated.name,
          workspaceRoot: updated.workspaceRoot,
          dbPath: updated.dbPath,
        },
      });
    }

    if (action === 'login') {
      if (!email || !password) {
        return NextResponse.json({ error: 'Email and password are required' }, { status: 400 });
      }

      const user = await db.user.findUnique({ where: { email } });
      if (!user || !user.password) {
        return NextResponse.json({ error: 'Invalid email or password' }, { status: 401 });
      }

      const isPasswordValid = await bcrypt.compare(password, user.password);
      if (!isPasswordValid) {
        return NextResponse.json({ error: 'Invalid email or password' }, { status: 401 });
      }

      const workspace = provisionUserWorkspace(user.id);
      const updated = await db.user.update({
        where: { id: user.id },
        data: {
          workspaceRoot: workspace.root,
          dbPath: workspace.dbPath,
        },
      });

      const token = jwt.sign({ userId: updated.id, email: updated.email }, JWT_SECRET, { expiresIn: '7d' });

      return NextResponse.json({
        success: true,
        token,
        user: {
          id: updated.id,
          email: updated.email,
          name: updated.name,
          workspaceRoot: updated.workspaceRoot,
          dbPath: updated.dbPath,
        },
      });
    }

    if (action === 'verify') {
      const { token } = body;
      if (!token) {
        return NextResponse.json({ error: 'Token is required' }, { status: 400 });
      }

      try {
        const decoded = jwt.verify(token, JWT_SECRET) as { userId: string; email: string };
        const user = await db.user.findUnique({ where: { id: decoded.userId } });
        if (!user) {
          return NextResponse.json({ error: 'User not found' }, { status: 401 });
        }

        return NextResponse.json({
          success: true,
          user: {
            id: user.id,
            email: user.email,
            name: user.name,
            workspaceRoot: user.workspaceRoot,
            dbPath: user.dbPath,
          },
        });
      } catch {
        return NextResponse.json({ error: 'Invalid or expired token' }, { status: 401 });
      }
    }

    return NextResponse.json({ error: 'Invalid action' }, { status: 400 });
  } catch (error) {
    console.error('Auth API error:', error);
    return NextResponse.json({ error: 'Authentication failed' }, { status: 500 });
  }
}

export async function GET() {
  return NextResponse.json({ authenticated: false, user: null });
}
